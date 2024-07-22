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
