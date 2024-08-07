from fastapi import FastAPI, APIRouter, HTTPException
from neo4j import GraphDatabase, basic_auth
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

# Default Neo4j database configuration
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"

# Create FastAPI instance
app = FastAPI()

# Create a router instance
router = APIRouter()

# Pydantic models
class RelationshipProperty(BaseModel):
    value: Any
    type: str

class Relationship(BaseModel):
    id: int
    type: str
    start_node_id: int
    end_node_id: int
    properties: Dict[str, RelationshipProperty]

class NodeProperty(BaseModel):
    value: Any
    type: str

class Node(BaseModel):
    id: int
    labels: List[str]
    properties: Dict[str, NodeProperty]
    relationships: List[Relationship] = []

class DbCredentials(BaseModel):
    uri: Optional[str] = DEFAULT_NEO4J_URI
    user: Optional[str] = DEFAULT_NEO4J_USER
    password: Optional[str] = DEFAULT_NEO4J_PASSWORD

# Create Neo4j driver
def get_neo4j_driver(credentials: DbCredentials):
    driver = GraphDatabase.driver(credentials.uri, auth=basic_auth(credentials.user, credentials.password))
    return driver

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

# Serialize properties
def serialize_property(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value

# Fetch schema information
def fetch_schema(driver):
    query = "CALL db.schema.visualization()"
    with driver.session() as session:
        result = session.run(query)
        nodes = result.single()["nodes"]
        relationships = result.single()["relationships"]

        node_properties = {}
        relationship_properties = {}

        for node in nodes:
            labels = ":".join(node["labels"])
            node_properties[labels] = [prop["propertyKey"] for prop in node["properties"]]

        for relationship in relationships:
            rel_type = relationship["type"]
            relationship_properties[rel_type] = [prop["propertyKey"] for prop in relationship["properties"]]

        return node_properties, relationship_properties

# Fetch nodes and relationships
def fetch_nodes_and_relationships_from_neo4j(driver):
    node_properties, relationship_properties = fetch_schema(driver)

    node_query_parts = []
    for labels, properties in node_properties.items():
        for prop in properties:
            node_query_parts.append(f"{{key: '{prop}', value: n.`{prop}`, type: type(n.`{prop}`).toString()}}")

    rel_query_parts = []
    for rel_type, properties in relationship_properties.items():
        for prop in properties:
            rel_query_parts.append(f"{{key: '{prop}', value: r.`{prop}`, type: type(r.`{prop}`).toString()}}")

    query = f"""
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n, collect(r) as relationships, collect(m) as related_nodes,
           [{', '.join(node_query_parts)}] AS node_properties,
           [rel IN collect(r) | [{', '.join(rel_query_parts)}]] AS rel_properties
    LIMIT 10
    """

    with driver.session() as session:
        result = session.run(query)
        nodes_dict = {}
        for record in result:
            node = record["n"]
            relationships = record["relationships"]
            related_nodes = record["related_nodes"]
            node_properties = {prop["key"]: NodeProperty(value=serialize_property(prop["value"]), type=prop["type"]) for prop in record["node_properties"]}

            if node.id not in nodes_dict:
                nodes_dict[node.id] = Node(
                    id=node.id,
                    labels=node.labels,
                    properties=node_properties,
                    relationships=[]
                )

            for rel, related_node, rel_props in zip(relationships, related_nodes, record["rel_properties"]):
                if rel:
                    rel_properties = {prop["key"]: RelationshipProperty(value=serialize_property(prop["value"]), type=prop["type"]) for prop in rel_props}
                    relationship = Relationship(
                        id=rel.id,
                        type=rel.type,
                        start_node_id=rel.start_node_id,
                        end_node_id=rel.end_node_id,
                        properties=rel_properties
                    )
                    nodes_dict[node.id].relationships.append(relationship)

        return list(nodes_dict.values())

# Define the endpoint to fetch nodes and relationships
@router.post("/nodes", response_model=List[Node])
async def get_nodes(credentials: DbCredentials = DbCredentials()):
    try:
        driver = get_neo4j_driver(credentials)
        nodes = fetch_nodes_and_relationships_from_neo4j(driver)
        return nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()

# Include the router in the FastAPI app
app.include_router(router, prefix="/api")

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
