from pymongo import MongoClient
from neo4j import GraphDatabase
from config import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_NAME,
    NEO4J_URI,
    NEO4J_USER,
    NEO4J_PASSWORD
)


# MongoDB


mongo_client = MongoClient(MONGO_URI)

mongo_db = mongo_client[DB_NAME]

films_collection = mongo_db[COLLECTION_NAME]


def get_db():
    return mongo_db


def get_collection():
    return films_collection



# Neo4j

neo4j_driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)


def get_neo4j_driver():
    return neo4j_driver