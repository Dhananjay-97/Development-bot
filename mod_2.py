import logging
import json
from fastapi import FastAPI, APIRouter, HTTPException, Depends
from neo4j import GraphDatabase, basic_auth
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path

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

# File path for storing fetched data
DATA_FILE_PATH = Path("fetched_data.json")

class Relationship(BaseModel):
    type: Optional[str]
    start_node_labels: Optional[List[str]]
    end_node_labels: Optional[List[str]]

class LabelDetails(BaseModel):
    properties: Dict[str, str] = Field(default_factory=dict)
    relationships: List[Relationship] = Field(default_factory=list)

class NodeResponse(BaseModel):
    node_labels: List[str] = Field(default_factory=list)
    label_info: Dict[str, LabelDetails] = Field(default_factory=dict)

class RelationshipStatement(BaseModel):
    statement: str

class DbCredentials(BaseModel):
    uri: Optional[str] = Field(DEFAULT_NEO4J_URI)
    user: Optional[str] = Field(DEFAULT_NEO4J_USER)
    password: Optional[str] = Field(DEFAULT_NEO4J_PASSWORD)

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
    query = """
    CALL db.schema.visualization()
    """
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
        relationships = record.get("relationships", [])
        node_properties = {}
        schema_relationships = []

        for node in nodes:
            labels = list(node.get("labels", []))
            for label in labels:
                if label not in node_properties:
                    node_properties[label] = {prop["propertyKey"]: determine_type(prop["propertyValue"]) for prop in node.get("properties", [])}

        for rel in relationships:
            schema_relationships.append({
                "type": rel.get("type"),
                "start_node_labels": list(rel.get("startNode", {}).get("labels", [])),
                "end_node_labels": list(rel.get("endNode", {}).get("labels", []))
            })

        logger.info(f"Fetched schema properties for nodes: {node_properties}")
        logger.info(f"Fetched schema relationships: {schema_relationships}")
        return node_properties, schema_relationships

def fetch_nodes_and_relationships_from_neo4j(driver, node_properties):
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n, labels(n) AS labels, keys(n) AS prop_keys, [key IN keys(n) | n[key]] AS prop_values, 
           collect(DISTINCT {type: type(r), start_node_labels: labels(startNode(r)), end_node_labels: labels(endNode(r))}) AS relationships
    LIMIT 10
    """
    logger.info("Fetching nodes and relationships from Neo4j")
    with driver.session() as session:
        result = session.run(query)
        labels_dict = {label: {"properties": {}, "relationships": []} for label in node_properties.keys()}
        for record in result:
            labels = record["labels"]
            prop_keys = record["prop_keys"]
            prop_values = record["prop_values"]
            relationships = record["relationships"]

            logger.info(f"Processing record with labels: {labels}")

            node_properties_dict = {prop_key: determine_type(prop_value) for prop_key, prop_value in zip(prop_keys, prop_values)}

            for label in labels:
                if label in labels_dict:
                    logger.info(f"Adding properties to label {label}")
                    # Merge properties
                    for key, value in zip(prop_keys, prop_values):
                        labels_dict[label]["properties"][key] = determine_type(value)
                    # Add relationships
                    for rel in relationships:
                        relationship = Relationship(
                            type=rel.get('type'),
                            start_node_labels=rel.get('start_node_labels'),
                            end_node_labels=rel.get('end_node_labels')
                        )
                        labels_dict[label]["relationships"].append(relationship)

        node_labels = list(labels_dict.keys())
        nodes_list = {"node_labels": node_labels, "label_info": labels_dict}
        logger.info(f"Fetched nodes: {nodes_list}")
        return nodes_list

def dump_data_to_file(data: NodeResponse, file_path: Path):
    with file_path.open("w") as file:
        json.dump(data.dict(), file)
    logger.info(f"Data dumped to {file_path}")

def load_data_from_file(file_path: Path) -> NodeResponse:
    with file_path.open("r") as file:
        data = json.load(file)
    logger.info(f"Data loaded from {file_path}")
    return NodeResponse(**data)

@router.post("/nodes", response_model=NodeResponse)
async def get_nodes(credentials: DbCredentials = Depends()):
    try:
        logger.info("Received request to fetch nodes and relationships")
        driver = get_neo4j_driver(credentials)
        node_properties, _ = fetch_schema(driver)
        logger.info(f"Node properties: {node_properties}")
        nodes = fetch_nodes_and_relationships_from_neo4j(driver, node_properties)
        logger.info(f"Final nodes response: {nodes}")
        dump_data_to_file(nodes, DATA_FILE_PATH)  # Store fetched data in a JSON file
        return nodes
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()
        logger.info("Closed Neo4j driver")

class LabelInfoResponse(BaseModel):
    label: str
    properties: Dict[str, str]
    relationships: List[RelationshipStatement]

def generate_relationship_statements(labels: List[str], relationships: List[Relationship]) -> List[RelationshipStatement]:
    relationship_statements = []
    for rel in relationships:
        if any(lbl in labels for lbl in rel.start_node_labels) and any(lbl in labels for lbl in rel.end_node_labels):
            start_labels = ':'.join(rel.start_node_labels)
            end_labels = ':'.join(rel.end_node_labels)
            statement = f"({start_labels})-[:{rel.type}]->({end_labels})"
            relationship_statements.append(RelationshipStatement(statement=statement))
            logger.info(f"Generated statement: {statement}")
    return relationship_statements

@router.post("/label_info", response_model=List[LabelInfoResponse])
async def get_label_info(
    labels: List[str]
):
    try:
        logger.info(f"Received request to fetch label info for labels: {labels}")

        data = load_data_from_file(DATA_FILE_PATH)

        label_info = data.label_info

        label_info_responses = []

        for label in labels:
            if label not in label_info:
                continue
            properties = label_info[label].properties
            relationships = label_info[label].relationships

            relationship_statements = generate_relationship_statements(labels, relationships)

            label_info_responses.append(LabelInfoResponse(
                label=label,
                properties=properties,
                relationships=relationship_statements
            ))

        return label_info_responses
    except Exception as e:
        logger.error(f"Error fetching label info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

app.include_router(router, prefix="/api")

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
