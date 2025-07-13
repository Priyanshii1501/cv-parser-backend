# db.py
import os
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from dotenv import load_dotenv

load_dotenv()
MONGODB_URL = os.getenv("MONGODB_URL")
DB_NAME = os.getenv("MONGODB_DB", "cvparser")

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None

def connect_db():
    global _client, _db
    _client = AsyncIOMotorClient(MONGODB_URL, serverSelectionTimeoutMS=5000)
    _db = _client[DB_NAME]
    try:
        _client.admin.command("ping")
        print("Connected to MongoDB!")
    except Exception as e:
        print("MongoDB connection failed:", e)

def close_db():
    if _client:
        _client.close()
        print("MongoDB connection closed")

def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database not connected")
    return _db
