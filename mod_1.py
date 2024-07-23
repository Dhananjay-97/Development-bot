import logging
from fastapi import FastAPI, APIRouter, HTTPException
from neo4j import GraphDatabase, basic_auth
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default Neo4j database configuration
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"

# Create FastAPI instance
app = FastAPI()

# Create a router instance
router = APIRouter()

# Pydantic models
class Relationship(BaseModel):
    id: int
    type: str
    start_node_id: int
    end_node_id: int
    properties: Dict[str, Any]

class Node(BaseModel):
    id: int
    labels: List[str]
    properties: Dict[str, Any]  # Dictionary of property names to their values
    relationships: List[Relationship] = []  # List of relationships

class DbCredentials(BaseModel):
    uri: Optional[str] = DEFAULT_NEO4J_URI
    user: Optional[str] = DEFAULT_NEO4J_USER
    password: Optional[str] = DEFAULT_NEO4J_PASSWORD

# Create Neo4j driver
def get_neo4j_driver(credentials: DbCredentials):
    logger.info(f"Initializing Neo4j driver with URI: {credentials.uri}")
    driver = GraphDatabase.driver(credentials.uri, auth=basic_auth(credentials.user, credentials.password))
    return driver

# Fetch schema information
def fetch_schema(driver):
    query = "CALL db.schema.visualization()"
    logger.info("Fetching schema information")
    with driver.session() as session:
        result = session.run(query)
        if not result:
            logger.warning("No schema information returned")
            return {}

        record = result.single()
        if not record:
            logger.warning("Schema visualization query returned no records")
            return {}

        nodes = record.get("nodes", [])
        node_properties = {}

        for node in nodes:
            labels = list(node.get("labels", []))
            labels_key = ":".join([label for label in labels if label is not None])
            node_properties[labels_key] = {prop["propertyKey"]: determine_type(prop["propertyValue"]) for prop in node.get("properties", [])}

        logger.info(f"Fetched schema properties for nodes: {node_properties}")
        return node_properties

# Determine the property type
def determine_type(value):
    if isinstance(value, datetime):
        return "datetime"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, bool):
        return "boolean"
    else:
        return "string"

# Fetch nodes and relationships from Neo4j
def fetch_nodes_and_relationships_from_neo4j(driver, node_properties):
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n, keys(n) AS prop_keys, [key in keys(n) | n[key]] AS prop_values, collect(r) AS relationships, collect(m) AS related_nodes
    LIMIT 10
    """
    logger.info("Fetching nodes and relationships from Neo4j")
    with driver.session() as session:
        result = session.run(query)
        nodes_list = []
        for record in result:
            node = record["n"]
            prop_keys = record["prop_keys"]
            prop_values = record["prop_values"]
            relationships = record["relationships"]
            related_nodes = record["related_nodes"]

            labels = list(node.labels) if node.labels is not None else []
            labels_key = ":".join([label for label in labels if label is not None])
            node_props_schema = node_properties.get(labels_key, {})

            node_properties_dict = {prop_key: prop_value for prop_key, prop_value in zip(prop_keys, prop_values)}

            relationships_list = []
            for rel, rel_node in zip(relationships, related_nodes):
                if rel:
                    rel_properties = {key: rel[key] for key in rel.keys() if key not in {"id", "type", "start_node_id", "end_node_id"}}
                    relationship = Relationship(
                        id=rel.id,
                        type=rel.type,
                        start_node_id=rel.start_node_id,
                        end_node_id=rel.end_node_id,
                        properties=rel_properties
                    )
                    relationships_list.append(relationship)

            nodes_list.append(Node(
                id=node.id,
                labels=labels,
                properties=node_properties_dict,
                relationships=relationships_list
            ))
        logger.info(f"Fetched nodes: {nodes_list}")
        return nodes_list

# Define the endpoint to fetch nodes and relationships
@router.post("/nodes", response_model=List[Node])
async def get_nodes(credentials: DbCredentials = DbCredentials()):
    try:
        logger.info("Received request to fetch nodes and relationships")
        driver = get_neo4j_driver(credentials)
        node_properties = fetch_schema(driver)
        nodes = fetch_nodes_and_relationships_from_neo4j(driver, node_properties)
        return nodes
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()
        logger.info("Closed Neo4j driver")

# Include the router in the FastAPI app
app.include_router(router, prefix="/api")

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
