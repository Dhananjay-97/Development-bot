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

class Relationship(BaseModel):
    type: Optional[str]
    start_node_labels: Optional[List[str]]
    end_node_labels: Optional[List[str]]

class NodeProperties(BaseModel):
    properties: Dict[str, Any]
    relationships: List[Relationship]

class NodeResponse(BaseModel):
    node_labels: List[str]
    labels: Dict[str, NodeProperties]

class DbCredentials(BaseModel):
    uri: Optional[str] = DEFAULT_NEO4J_URI
    user: Optional[str] = DEFAULT_NEO4J_USER
    password: Optional[str] = DEFAULT_NEO4J_PASSWORD
def get_neo4j_driver(credentials: DbCredentials):
    logger.info(f"Initializing Neo4j driver with URI: {credentials.uri}")
    driver = GraphDatabase.driver(credentials.uri, auth=basic_auth(credentials.user, credentials.password))
    return driver
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
        node_labels = set()
        node_properties = {}

        for node in nodes:
            labels = list(node.get("labels", []))
            node_labels.update(labels)
            labels_key = ":".join([label for label in labels if label is not None])
            node_properties[labels_key] = {prop["propertyKey"]: determine_type(prop["propertyValue"]) for prop in node.get("properties", [])}

        logger.info(f"Fetched schema properties for nodes: {node_properties}")
        return list(node_labels), node_properties
def fetch_nodes_and_relationships_from_neo4j(driver, node_labels, node_properties):
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    WITH n, labels(n) AS labels, keys(n) AS prop_keys, [key IN keys(n) | n[key]] AS prop_values, collect(r) AS relationships_data
    RETURN n, 
           labels, 
           prop_keys, 
           prop_values, 
           [rel IN relationships_data | {type: type(rel), start_node_labels: labels(startNode(rel)), end_node_labels: labels(endNode(rel))}] AS relationships
    LIMIT 10
    """
    logger.info("Fetching nodes and relationships from Neo4j")
    with driver.session() as session:
        result = session.run(query)
        labels_dict = {label: {"properties": {}, "relationships": []} for label in node_labels}
        for record in result:
            node = record["n"]
            labels = record["labels"]
            prop_keys = record["prop_keys"]
            prop_values = record["prop_values"]
            relationships = record["relationships"]

            node_properties_dict = {prop_key: prop_value for prop_key, prop_value in zip(prop_keys, prop_values)}

            relationships_list = []
            for rel in relationships:
                relationship = Relationship(
                    type=rel.get('type'),
                    start_node_labels=rel.get('start_node_labels'),
                    end_node_labels=rel.get('end_node_labels')
                )
                relationships_list.append(relationship)

            for label in labels:
                labels_dict[label]["properties"] = node_properties_dict
                labels_dict[label]["relationships"] = relationships_list

        nodes_list = [node_labels, labels_dict]
        logger.info(f"Fetched nodes: {nodes_list}")
        return nodes_list
@router.post("/nodes", response_model=NodeResponse)
async def get_nodes(credentials: DbCredentials = DbCredentials()):
    try:
        logger.info("Received request to fetch nodes and relationships")
        driver = get_neo4j_driver(credentials)
        node_labels, node_properties = fetch_schema(driver)
        nodes = fetch_nodes_and_relationships_from_neo4j(driver, node_labels, node_properties)
        return nodes
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()
        logger.info("Closed Neo4j driver")
app.include_router(router, prefix="/api")

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
