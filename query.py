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
