from fastapi import FastAPI, Query, UploadFile, Depends, File, HTTPException, Request, Header, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber, logging, io, os, json, re, requests, ssl, certifi, hmac, hashlib, base64
from docx import Document
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv
import google.generativeai as genai
from hubspot import HubSpot
from hubspot.crm.properties import PropertyCreate
from hubspot.crm.contacts import (
    PublicObjectSearchRequest, Filter, FilterGroup,
    SimplePublicObjectInputForCreate, SimplePublicObjectInput
)
from typing import List
from motor.motor_asyncio import AsyncIOMotorDatabase
from database import connect_db, close_db, get_db
from pymongo import MongoClient

load_dotenv()

app = FastAPI()

# Configure SSL context
ssl_context = ssl.create_default_context(cafile=certifi.where())
os.environ['SSL_CERT_FILE'] = certifi.where()

# Then modify your HubSpot client initialization:
hubspot_client = HubSpot(
    access_token=os.getenv("HUBSPOT_TOKEN"),
    api_client_configuration={"ssl_context": ssl_context}
)

CLIENT_SECRET = os.getenv("HUBSPOT_CLIENT_SECRET")

sync_mongo = MongoClient(os.getenv("MONGODB_URL"))
sync_db = sync_mongo["cvparser"]["resumes"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')  # Optional: log to file
    ]
)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or set to your frontend URL like ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#hubspot token
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")

# Initialize Gemini client
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
MODEL = "gemini-2.5-flash"
model = genai.GenerativeModel(model_name=MODEL)
hubspot_client = HubSpot(access_token=os.getenv("HUBSPOT_TOKEN"))
FOLDER_ID="249026326717"

# Improved prompt: prevents Gemini from wrapping response in code blocks


RESUME_PROMPT = """
You are an advanced resume parser.

Your task is to analyze the following resume text and extract only a valid JSON object with the following schema. You must:

- Identify the full name, email, and phone number from the contact section
- Identify the current or most recent job title and company
- Extract a list of relevant technical and soft skills
- Summarize the candidate's professional experience in 1-2 lines
- Extract the most recent location from the contact section or current job (with proper normalization)
- Calculate and return:
  - total_experience: A descriptive string value of total work experience (e.g., "7+ years of experience")
  - year_of_experience: A whole number (e.g., 7), rounded **up** if experience is fractional

To calculate total experience:
- Identify and normalize all date ranges (e.g., "Jan 2020 - Present", "03/2017 - 12/2020", etc.)
- Include only relevant full-time work experience (skip internships or academic projects unless stated as full-time)
- Merge overlapping periods and sum non-contiguous experience
- Round fractional years **up** to the next whole number

Normalize all fields:
- Convert abbreviated state and country names to their full official forms (e.g., "CA" → "California", "USA" → "United States")
- Use proper case for names and city/state

Output strictly a valid JSON object in the following schema. Use empty strings (`""`) or empty lists (`[]`) if values are not found. Do not include any extra formatting or text outside the JSON.

{
  "name": "",
  "email": "",
  "phone": "",
  "job_title": "",
  "skills": [],
  "experience": "",
  "company": "",
  "location": {
    "city": "",
    "state": "",
    "country": ""
  },
  "total_experience": "",
  "year_of_experience": ""
}

Resume Text:
"""

def extract_text_from_pdf(file_bytes):
    text = ""
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text

def extract_text_from_docx(file_bytes):
    with NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
        tmp.write(file_bytes)
        tmp.flush()
        doc = Document(tmp.name)
    return "\n".join([p.text for p in doc.paragraphs if p.text.strip()])



def upload_bytes_to_hs(data: bytes, filename: str, folder_id: str) -> str:
    url = "https://api.hubapi.com/files/v3/files"
    headers = {
        "Authorization": f"Bearer {os.getenv('HUBSPOT_TOKEN')}"
    }

    options = {
        "access": "PRIVATE",
        "overwrite": False,
    }

    files = {
        "file": (filename, io.BytesIO(data)),
        "fileName": (None, filename),
        "folderId": (None, folder_id),
        "access": (None, "PRIVATE"),
        "overwrite": (None, "false"),
        "options": (None, json.dumps(options), "application/json")
    }

    resp = requests.post(url, headers=headers, files=files)
    print(resp)

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        raise HTTPException(status_code=resp.status_code, detail=f"HubSpot upload failed: {resp.text}")

    file_url = resp.json().get("url")
    if not file_url:
        raise HTTPException(status_code=500, detail="File uploaded, but URL not returned by HubSpot.")

    return file_url






def update_dropdown_property(property_name: str, label: str, value: str) -> str:
    """
    Updates a HubSpot dropdown property (enumeration) by adding the value if it doesn't exist.
    Returns the selected value.
    """
    try:
        logger.info(f"Updating '{property_name}' in HubSpot")

        # Get existing property options
        prop = hubspot_client.crm.properties.core_api.get_by_name(
            object_type="contacts", property_name=property_name
        )
        existing = {opt.value for opt in prop.options}

        # Clean and format value
        incoming_value = value.strip().title()
        if not incoming_value:
            return ""

        combined = existing.union({incoming_value})

        opts_payload = [{"label": v, "value": v} for v in sorted(combined)]

        update_prop = PropertyCreate(
            name=property_name,
            label=label,
            group_name="contactinformation",
            type="enumeration",
            field_type="select",
            options=opts_payload
        )

        hubspot_client.crm.properties.core_api.update(
            object_type="contacts",
            property_name=property_name,
            property_update=update_prop
        )

        return incoming_value
    except Exception as e:
        logger.error(f"Failed to update {property_name} dropdown: {str(e)}", exc_info=True)
        return ""




# def update_keywords_property(keywords_list):
#     """Create or update the keywords property in HubSpot"""
#     if not keywords_list:
#         return []

#     try:
#         existing_options = set()
#         new_keywords = {k.strip() for k in keywords_list if k.strip()}
        
#         # First try to get existing property
#         try:
#             prop = hubspot_client.crm.properties.core_api.get_by_name(
#                 object_type="contacts",
#                 property_name="keywords_domains"
#             )
#             existing_options = {opt.value for opt in prop.options}
#         except Exception as e:
#             logger.info("Creating new 'keywords_domains' property in HubSpot")
#             # Property doesn't exist, create it with initial keywords
#             create_prop = {
#                 "name": "keywords_domains",
#                 "label": "keywords_domains",
#                 "groupName": "contactinformation",
#                 "type": "enumeration",
#                 "fieldType": "checkbox",
#                 "options": [{"label": k, "value": k} for k in sorted(new_keywords)]
#             }
#             hubspot_client.crm.properties.core_api.create(
#                 object_type="contacts",
#                 property_create=create_prop
#             )
#             return list(new_keywords)

#         # If we get here, property exists - update with any new keywords
#         all_keywords = existing_options.union(new_keywords)
        
#         # Only update if there are new keywords to add
#         if new_keywords - existing_options:
#             update_prop = {
#                 "options": [{"label": k, "value": k} for k in sorted(all_keywords)]
#             }
#             hubspot_client.crm.properties.core_api.update(
#                 object_type="contacts",
#                 property_name="keywords_domains",
#                 property_update=update_prop
#             )

#         return list(new_keywords - existing_options)

#     except Exception as e:
#         logger.error(f"Failed to update keywords property: {str(e)}")
#         return []


# Startup and shutdown events
@app.on_event("startup")
async def on_start():
    connect_db()


@app.on_event("shutdown")
def on_stop():
    close_db()


@app.post("/parse_resume/")
async def parse_resume(file: UploadFile = File(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    try:
        logger.info(f"Starting resume processing for file: {file.filename}")
        data = await file.read()
        content_type = file.content_type
        text = ""

        # Text extraction
        try:
            logger.info("Extracting text from file")
            if content_type == "application/pdf":
                text = extract_text_from_pdf(data)
            elif content_type in [
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/msword"
            ]:
                text = extract_text_from_docx(data)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format.")
        except Exception as e:
            logger.error(f"Text extraction failed: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to extract text: {str(e)}")

        if not text.strip():
            logger.warning("No text extracted from file")
            raise HTTPException(status_code=400, detail="No text extracted from the file.")
        
        # File upload to HubSpot
        try:
            logger.info("Uploading file to HubSpot")
            file_url = upload_bytes_to_hs(data, file.filename, FOLDER_ID)
        except Exception as e:
            logger.error(f"HubSpot file upload failed: {str(e)}", exc_info=True)
            raise HTTPException(500, f"File upload failed: {str(e)}")

        # Compose prompt
        prompt = RESUME_PROMPT + "\n\n" + text

        try:
            logger.info("Sending to Gemini for parsing")
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,
                    "response_mime_type": "application/json"
                }
            )

            raw = response.text.strip()
            # safe cleanup
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Gemini response: {raw}", exc_info=True)
                raise HTTPException(status_code=500, detail="Gemini returned invalid JSON.")

            name = parsed.get("name", "").strip()
            parts = name.split()
            firstname = parts[0] if parts else ""
            lastname = " ".join(parts[1:]) if len(parts) > 1 else ""

            location_data = parsed.get("location", {})
            city = location_data.get("city", "").strip()
            state = location_data.get("state", "").strip()
            country = location_data.get("country", "").strip()

            selected_city = update_dropdown_property("city_dropdown", "city", city)
            selected_state = update_dropdown_property("state_dropdown", "state", state)
            selected_country = update_dropdown_property("country_dropdown", "country", country)

            # Skills setup
            try:
                logger.info("Updating skills in HubSpot")
                prop = hubspot_client.crm.properties.core_api.get_by_name(
                    object_type="contacts", property_name="skills"
                )

                existing = {opt.value for opt in prop.options}
                incoming = set(parsed.get("skills", []))
                combined = existing.union(incoming)

                opts_payload = [{"label": v, "value": v} for v in sorted(combined)]

                update_prop = PropertyCreate(
                    name="skills",
                    label="Skills",
                    group_name="contactinformation",
                    type="enumeration",
                    field_type="checkbox",
                    options=opts_payload
                )
                response = hubspot_client.crm.properties.core_api.update(
                    object_type="contacts",
                    property_name="skills",
                    property_update=update_prop
                )

                selected = incoming
                skills_str = ";".join(selected)

                # In your parse_resume function, replace the keywords section with:

                # logger.info("Updating keywords in HubSpot")
                # keywords = parsed.get("keywords", [])
                # selected_keywords = update_keywords_property(keywords)
                # keywords_str = ";".join(selected_keywords) if selected_keywords else ""


                email = parsed["email"]
                total_experience = parsed["total_experience"]
                year_of_experience = parsed["year_of_experience"] 


                req = PublicObjectSearchRequest(
                    filter_groups=[FilterGroup(filters=[Filter(property_name="email", operator="EQ", value=email)])],
                    properties=["email"], limit=1
                )

                search_res = hubspot_client.crm.contacts.search_api.do_search(public_object_search_request=req)
                if search_res.results:
                    logger.info(f"Updating existing contact: {email}")
                    contact_id = search_res.results[0].id
                    hubspot_client.crm.contacts.basic_api.update(
                        contact_id,
                        simple_public_object_input=SimplePublicObjectInput(
                            properties={
                                "firstname": firstname,
                                "lastname": lastname,
                                "phone": parsed["phone"],
                                "jobtitle": parsed["job_title"],
                                "company": parsed["company"],
                                "skills": skills_str,
                                # "keywords_domains" : keywords_str,
                                "cv_url_link": file_url,
                                "total_experience" : total_experience,
                                "year_of_experience" : year_of_experience,
                                "city_dropdown": selected_city,
                                "state_dropdown": selected_state,
                                "country_dropdown": selected_country

                            }
                        )
                    )
                else:
                    logger.info(f"Creating new contact: {email}")
                    in_props = {
                        "firstname": firstname,
                        "lastname": lastname,
                        "email": email,
                        "phone": parsed["phone"],
                        "jobtitle": parsed["job_title"],
                        "company": parsed["company"],
                        "skills": skills_str,
                        "total_experience" : total_experience,
                        "year_of_experience" : year_of_experience,
                        "cv_url_link": file_url,
                        "city_dropdown": selected_city,
                        "state_dropdown": selected_state,
                        "country_dropdown": selected_country,
                        # "keywords_domains" : keywords_str,
                    }
                    hs_obj = hubspot_client.crm.contacts.basic_api.create(
                        simple_public_object_input_for_create=SimplePublicObjectInputForCreate(properties=in_props)
                    )
                    contact_id = hs_obj.id

                logger.info(f"Successfully processed resume for {email}")

                full_text = f"{parsed['name']} {parsed['email']} {parsed['job_title']} {parsed['skills']} {text}"

                await db["resumes"].update_one(
                    {"contact_id": contact_id},
                    {"$set": {
                        "contact_id": contact_id,
                        "name": parsed["name"],
                        "email": parsed["email"],
                        "job_title": parsed["job_title"],
                        "skills": parsed["skills"],
                        "extracted_text": text,
                        "full_text": full_text,
                    }},
                    upsert=True
                )

                logger.info(f"Successfully added resume data for {email} in database.")

                return JSONResponse(content=parsed)

            except Exception as e:
                logger.error(f"HubSpot operation failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"HubSpot operation failed: {str(e)}")

        except Exception as e:
            logger.error(f"AI parsing failed: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"AI parsing failed: {str(e)}")

    except HTTPException:
        raise  # Re-raise FastAPI HTTP exceptions
    except Exception as e:
        logger.error(f"Unexpected error in parse_resume: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    

@app.get("/search/")
async def search_resumes(
    keywords: List[str] = Query(...),
    mode: str = Query("or", regex="^(and|or)$"),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    # Build regex clauses
    clauses = []
    regexes = []  # Store compiled regex for reuse
    for kw in keywords:
        if re.fullmatch(r"[\w\s]+", kw):  # Safe for word boundaries
            pattern = rf"\b{re.escape(kw)}\b"
        else:  # For special chars like +, /, etc.
            pattern = re.escape(kw)
        regex = re.compile(pattern, re.IGNORECASE)
        regexes.append((kw, regex))
        clauses.append({"full_text": {"$regex": regex}})
    
    query = {"$or": clauses} if mode == "or" else {"$and": clauses}

    # Query and exclude _id
    projection = {
        "_id": 0,
        "full_text": 1,
        "name": 1,
        "job_title": 1,
        "skills": 1,
        "email": 1,
        "contact_id": 1
    }

    cursor = db["resumes"].find(query, projection)
    raw_results = await cursor.to_list(length=100)

    results = []

    for doc in raw_results:
        matched = []
        full_text = doc.get("full_text", "")
        for kw, rx in regexes:
            if rx.search(full_text):
                matched.append(kw)
        doc["matched_keywords"] = matched
        results.append(doc)

    return {
        "keywords": keywords,
        "mode": mode,
        "results": results
    }


@app.post("/webhook/hubspot", status_code=204)
async def handle_hubspot_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hubspot_signature_v3: str = Header(...),
    x_hubspot_request_timestamp: str = Header(...)
):
    raw_body = await request.body()
    raw_body_str = raw_body.decode('utf-8')
    url = str(request.url)
    source = f"POST{url}{raw_body_str}{x_hubspot_request_timestamp}"

    sig = base64.b64encode(hmac.new(
        CLIENT_SECRET.encode('utf-8'),
        msg=source.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()).decode()

    if not hmac.compare_digest(sig, x_hubspot_signature_v3):
        logger.warning("Invalid HubSpot webhook signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    events = await request.json()
    deleted_ids = [
        ev["objectId"]
        for ev in events
        if ev.get("subscriptionType") == "contact.privacyDeletion"
    ]
    logger(deleted_ids)
    if deleted_ids:
        background_tasks.add_task(remove_contacts, deleted_ids)

    return Response(status_code=204)


def remove_contacts(contact_ids: list[str]):
    result = sync_db.delete_many({"contact_id": {"$in": contact_ids}})
    logger.info(f"Removed {result.deleted_count} deleted HubSpot contact(s) from MongoDB")
