from neo4j import GraphDatabase

class Neo4JIngestor:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "neo4j", database: str = "neo4j") -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password), database = database)

    def close(self):
        self.driver.close()

    def wipe(self):
        result = self.driver.session().run(
            """
            CALL {
                MATCH (n)
                WITH n LIMIT 10000
                DETACH DELETE n
            } IN TRANSACTIONS;
            """
        )
        
    def splitToChunks(self, data, size:int = 50000):
        for i in range(0, len(data), size):
            yield data[i:i + size]
        
    def ingestEgoNetwork(
        self,
        ego_id: str,
        edges: list[tuple[str, str]],
        node_features: dict[str, list[str]], # We get all users from here
        ego_features: dict[str, list[str]], # Usually singular, but added for consistency
        circles: dict[str, list[str]]
    ):
        circs = [
            {
                "id": cname,
                "ego": ego_id
            }
            for cname in circles.keys()
        ]

        memberships = [
            {
                "user": userId,
                "circle": circleId
            }
            for circleId, users in circles.items()
            for userId in users
        ]

        users = [
            {"id": uid, "features": feats}
            for uid, feats in node_features.items()
        ]

        egoFollows = [{"src": ego_id, "dst": userId} for userId in node_features.keys()]

        follows = [{"src": a, "dst": b} for a, b in edges]

        with self.driver.session() as session:
            print("Adding constraints")
            result = session.run(
                """
                CREATE CONSTRAINT ego_pk IF NOT EXISTS
                FOR (e:Ego)
                REQUIRE (e.id) IS UNIQUE;
                """
            )
                
            result = session.run(
                """
                CREATE CONSTRAINT user_pk IF NOT EXISTS
                FOR (u:User)
                REQUIRE (u.id) IS UNIQUE;
                """
            )
            
            result = session.run(
                """
                CREATE CONSTRAINT circle_pk IF NOT EXISTS
                FOR (c:Circle)
                REQUIRE (c.id) IS UNIQUE;
                """
            )

            print("Adding ego")
            result = session.run(
                """
                MERGE (e:Ego {id: $ego})
                SET e.features = $features
                """,
                ego = ego_id,
                features = list(ego_features[ego_id])
            )

            print("Adding users")
            for userChunk in self.splitToChunks(users):
                result = session.run(
                    """
                    UNWIND $users as n
                    MERGE (u:User {id: n.id})
                    SET u.features = n.features
                    """,
                    users = userChunk
                )

            print("Adding circles")

            for circleChunk in self.splitToChunks(circs):
                result = session.run(
                    """
                    UNWIND $circles as n
                    MERGE (c:Circle {id: n.id})
                    WITH n,c
                    MATCH (e:Ego {id: n.ego})
                    MERGE (e)-[:OWNS]->(c)
                    """,
                    circles = circleChunk
                )

            print("Connecting nodes")

            for edgesChunk in self.splitToChunks(egoFollows):
                result = session.run(
                    """
                    UNWIND $edges as n
                    MATCH (e:Ego {id: n.src})
                    MATCH (u:User {id: n.dst})
                    MERGE (e)-[:FOLLOWS]->(u)
                    """,
                    edges = edgesChunk
                )

            for edgesChunk in self.splitToChunks(follows):
                result = session.run(
                    """
                    UNWIND $edges as n
                    MATCH (a:User {id: n.src})
                    MATCH (b:User {id: n.dst})
                    MERGE (a)-[:FOLLOWS]->(b)
                    """,
                    edges = edgesChunk
                )

            for memberChunk in self.splitToChunks(memberships):
                result = session.run(
                    """
                    UNWIND $membership as n
                    MATCH (u:User {id: n.user})
                    MATCH (c:Circle {id: n.circle})
                    MERGE (u)-[:PART_OF]->(c)
                    """,
                    membership = memberChunk
                )

