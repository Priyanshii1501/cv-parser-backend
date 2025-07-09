from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pdfplumber
import logging
import io
import os
from docx import Document
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv
import google.generativeai as genai
import requests
import json
import re
from hubspot import HubSpot
from hubspot.crm.properties import PropertyCreate
from hubspot.crm.contacts import (
    PublicObjectSearchRequest, Filter, FilterGroup,
    SimplePublicObjectInputForCreate, SimplePublicObjectInput
)
import ssl
import certifi
import os

# Configure SSL context
ssl_context = ssl.create_default_context(cafile=certifi.where())
os.environ['SSL_CERT_FILE'] = certifi.where()

# Then modify your HubSpot client initialization:
hubspot_client = HubSpot(
    access_token=os.getenv("HUBSPOT_TOKEN"),
    api_client_configuration={"ssl_context": ssl_context}
)

load_dotenv()

app = FastAPI()

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
You are a resume parser. Extract the following fields from the resume text below and output ONLY a VALID JSON object that exactly matches this schema (no extra text, formatting, or comments):

Analyze the provided resume text and extract the current location information according to these strict rules:

1. Primary Source: First check the contact information section (looking for address fields, phone/email nearby text, or header/footer content)
2. Secondary Source: If not found, use the location from the most recent job/experience entry
3. Normalization: 
   - Convert all location names to their official full forms (e.g., "CA" → "California", "NYC" → "New York City")
   - Use standard country names (e.g., "USA" → "United States", "UK" → "United Kingdom")
4. Priority: Always prefer current location markers over permanent addresses if both exist
5. Output Format: Return empty strings ("") for any missing fields

{
  "name": "",         // Full name of the candidate
  "email": "",        // Email address
  "phone": "",        // Contact number
  "job_title": "",    // Current or most recent job title
  "skills": [],       // List of key technical and soft skills
  "experience": "",   // Brief summary of professional experience
  "company": "",      // Current or most recent employer
  "location": ""      // City and state/country
  "total_experience": ""  //number of years of experience , text property please give accurate ans 
  "year_of_experience" : ""  //if number of experience is 11+ then put it as 11 , i want output as whole number , also do ceil of this number , i want number only
  "location": {
    "city": "",       // Extracted city name in proper case (e.g., "Mumbai")
    "state": "",      // Full state/province name (e.g., "Ontario")
    "country": ""     // Full country name (e.g., "Canada")

  "keywords": [],  // Auto-detected role-appropriate keywords
  "role_type": "",  // "technical", "non-technical", or "hybrid"
  }

}

Special Instructions:
- Ignore postal codes and street addresses unless they contain city/state names
- If location appears ambiguous (e.g., "Remote" or "Multiple locations"), return empty strings
- Verify consistency if location appears in multiple sections
- For countries, resolve abbreviations using ISO standards
- Reject inferred locations - only use explicitly stated information


KEYWORD EXTRACTION STRATEGY:
Please dont change any keywords , put as it is from resume dont add or delete keywords
1. ROLE DETECTION PHASE:
   - Analyze job_title and experience to determine:
     * Technical (SWE, Data Scientist, DevOps)
     * Non-technical (HR, Sales, Marketing)
     * Hybrid (Product Manager, Analytics)

2. DYNAMIC KEYWORD CATEGORIES:
   [For Technical Roles]:
   - Languages, Frameworks, Cloud, Databases
   - Tools, Certifications, Methodologies

   [For Non-Technical Roles]:
   - Domain Knowledge, Soft Skills
   - Industry Tools, Certifications
   - Process Knowledge

   [For Hybrid Roles]:
   - Combination of both technical and non-technical
   - Emphasize cross-functional skills

3. UNIVERSAL INCLUSIONS:
   - All mentioned technologies/tools
   - Certifications with full names
   - Industry buzzwords
   - Domain-specific terminology
   - Notable achievements/features

4. SMART PROCESSING RULES:
   - Auto-normalize without losing meaning:
     "React.js" → "React", "MS Excel" → "Excel"
   - Preserve proficiency levels when stated
   - Expand acronyms first occurrence:
     "CRM (Customer Relationship Management)"
   - Include version numbers for key tech
   - Tag industry domains:
     "(Healthcare)", "(FinTech)", "(E-commerce)"

EXAMPLE OUTPUTS:

1. Technical Role (Developer):
{
  "role_type": "technical",
  "keywords": [
    "Java 8",
    "Spring Boot",
    "Microservices",
    "AWS Certified",
    "Docker",
    "CI/CD Pipelines",
    "Agile Scrum",
    "Kubernetes",
    "(FinTech Domain)"
  ]
}

2. Non-Technical Role (HR):
{
  "role_type": "non-technical",
  "keywords": [
    "Talent Acquisition",
    "Workday HRIS",
    "Employee Engagement",
    "SHRM-CP Certified",
    "Compensation Benchmarking",
    "Labor Law Compliance",
    "(Healthcare Industry)"
  ]
}

3. Hybrid Role (Product Manager):
{
  "role_type": "hybrid",
  "keywords": [
    "Product Roadmapping",
    "JIRA Administration",
    "SQL Queries",
    "Stakeholder Management",
    "Market Analysis",
    "Python (Basic)",
    "(SaaS Experience)"
  ]
}

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




def update_keywords_property(keywords_list):
    """Create or update the keywords property in HubSpot"""
    if not keywords_list:
        return []

    try:
        existing_options = set()
        new_keywords = {k.strip() for k in keywords_list if k.strip()}
        
        # First try to get existing property
        try:
            prop = hubspot_client.crm.properties.core_api.get_by_name(
                object_type="contacts",
                property_name="keywords_domains"
            )
            existing_options = {opt.value for opt in prop.options}
        except Exception as e:
            logger.info("Creating new 'keywords_domains' property in HubSpot")
            # Property doesn't exist, create it with initial keywords
            create_prop = {
                "name": "keywords_domains",
                "label": "keywords_domains",
                "groupName": "contactinformation",
                "type": "enumeration",
                "fieldType": "checkbox",
                "options": [{"label": k, "value": k} for k in sorted(new_keywords)]
            }
            hubspot_client.crm.properties.core_api.create(
                object_type="contacts",
                property_create=create_prop
            )
            return list(new_keywords)

        # If we get here, property exists - update with any new keywords
        all_keywords = existing_options.union(new_keywords)
        
        # Only update if there are new keywords to add
        if new_keywords - existing_options:
            update_prop = {
                "options": [{"label": k, "value": k} for k in sorted(all_keywords)]
            }
            hubspot_client.crm.properties.core_api.update(
                object_type="contacts",
                property_name="keywords_domains",
                property_update=update_prop
            )

        return list(new_keywords - existing_options)

    except Exception as e:
        logger.error(f"Failed to update keywords property: {str(e)}")
        return []














@app.post("/parse_resume/")
async def parse_resume(file: UploadFile = File(...)):
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

                logger.info("Updating keywords in HubSpot")
                keywords = parsed.get("keywords", [])
                selected_keywords = update_keywords_property(keywords)
                keywords_str = ";".join(selected_keywords) if selected_keywords else ""


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
                                "keywords_domains" : keywords_str,
                                "resume_file_url": file_url,
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
                        "resume_file_url": file_url,
                        "city_dropdown": selected_city,
                        "state_dropdown": selected_state,
                        "country_dropdown": selected_country,
                        "keywords_domains" : keywords_str,
                    }
                    hs_obj = hubspot_client.crm.contacts.basic_api.create(
                        simple_public_object_input_for_create=SimplePublicObjectInputForCreate(properties=in_props)
                    )
                    contact_id = hs_obj.id

                logger.info(f"Successfully processed resume for {email}")
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