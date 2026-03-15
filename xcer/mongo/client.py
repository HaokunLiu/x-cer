"""MongoDB client management."""

import os
from pymongo import MongoClient
from xcer.paths import MONGODB_CONNECTION_STR_FILE


def get_mongodb_connection_str() -> str:
    """Get MongoDB connection string from environment or file."""
    connection_str = os.getenv("MONGODB_CONNECTION_STR")
    if connection_str:
        return connection_str

    if MONGODB_CONNECTION_STR_FILE.exists():
        with open(MONGODB_CONNECTION_STR_FILE, "r") as f:
            connection_str = f.read().strip()
            if connection_str:
                return connection_str

    raise ValueError(
        "MongoDB connection string not found. Please set MONGODB_CONNECTION_STR environment variable or create file at ~/.xcer/mongodb_connection_str.txt"
    )


def get_mongodb_client() -> MongoClient:
    """Get MongoDB client instance."""
    connection_str = get_mongodb_connection_str()
    client = MongoClient(connection_str)
    return client
