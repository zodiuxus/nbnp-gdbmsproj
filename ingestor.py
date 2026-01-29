def split_to_chunks(data, size:int = 50000):
    for i in range(0, len(data), size):
        yield data[i:i + size]

class Neo4JIngestor:
    def __init__(self, uri: str, user: str, password: str, database: str) -> None:
        from neo4j import GraphDatabase
        self.driver = GraphDatabase.driver(uri, auth=(user, password), database = database)

    def close(self):
        self.driver.close()

    def wipe(self):
        result = self.driver.session().run(
            """
            CALL {
                MATCH (n)
                DETACH DELETE n
            } IN TRANSACTIONS;
            """
        )
        
    def ingestEgoNetwork(
        self,
        ego_id: str,
        edges: list[tuple[str, str]],
        feats: list[tuple[str, str]],
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

        users = [{"id": uid} for uid in node_features.keys()]

        feat_groups = {name for name, _ in feats}
        feat_groups = [{"name": name} for name in feat_groups]

        feat_group_map = [{"gn": group_name, "fn": feature_name} for group_name, feature_name in feats]

        ego_follows = [{"src": ego_id, "dst": userId} for userId in node_features.keys()]

        feature_map = [{"id": node_id, "fn": feature_name} for node_id in node_features.keys() for feature_name in node_features[node_id]]
        ego_feature_map = [{"id": ego_id, "fn": feature_name} for feature_name in ego_features[ego_id]]

        follows = [{"src": a, "dst": b} for a, b in edges]

        with self.driver.session() as session:
            # print("Adding constraints")
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

            result = session.run(
                """
                CREATE CONSTRAINT featgroup_pk IF NOT EXISTS
                FOR (fg:FeatGroup)
                REQUIRE (fg.name) IS UNIQUE;
                """
            )

            # print("Adding ego")
            result = session.run(
                """
                MERGE (e:Ego {id: $ego})
                """, ego = ego_id,
            )

            # print("Adding users")
            for user_chunk in split_to_chunks(users):
                result = session.run(
                    """
                    UNWIND $users as n
                    MERGE (u:User {id: n.id})
                    """, users = user_chunk
                )

            users.clear()

            # print("Adding circles")

            for circle_chunk in split_to_chunks(circs):
                result = session.run(
                    """
                    UNWIND $circles as n
                    MERGE (c:Circle {id: n.id})
                    WITH n,c
                    MATCH (e:Ego {id: n.ego})
                    MERGE (e)-[:OWNS]->(c)
                    """, circles = circle_chunk
                )

            circs.clear()

            # print("Adding feature groups")
            for featgroup_chunk in split_to_chunks(feat_groups):
                result = session.run(
                    """
                    UNWIND $fg_names as f
                    MERGE (fg:FeatGroup {name: f.name})
                    """, fg_names = featgroup_chunk
                )

            feat_groups.clear()

            for feat_group_map_chunk in split_to_chunks(feat_group_map):
                result = session.run(
                    """
                    UNWIND $feats as f
                    MERGE (fn:FeatName {name: f.fn})
                    WITH f, fn
                    MATCH (fg:FeatGroup {name: f.gn})
                    MERGE (fg)-[:OWNS_FEAT]->(fn)
                    """, feats = feat_group_map_chunk
                )

            feat_group_map.clear()

            for feature_map_chunk in split_to_chunks(feature_map):
                result = session.run(
                    """
                    UNWIND $map as m
                    MATCH (u:User {id: m.id})
                    MATCH (fn:FeatName {name: m.fn})
                    MERGE (u)-[:HAS_FEAT]->(fn)
                    """, map = feature_map_chunk
                )

            feature_map.clear()

            result = session.run(
                """
                UNWIND $map as m
                MATCH (e:Ego {id: m.id})
                MATCH (fn:FeatName {name: m.fn})
                MERGE (e)-[:HAS_FEAT]->(fn)
                """, map = ego_feature_map
            )

            ego_feature_map.clear()

            # print("Connecting nodes")

            for edges_chunk in split_to_chunks(ego_follows):
                result = session.run(
                    """
                    UNWIND $edges as n
                    MATCH (e:Ego {id: n.src})
                    MATCH (u:User {id: n.dst})
                    MERGE (e)-[:FOLLOWS]->(u)
                    """, edges = edges_chunk
                )

            ego_follows.clear()

            for edges_chunk in split_to_chunks(follows):
                result = session.run(
                    """
                    UNWIND $edges as n
                    MATCH (a:User {id: n.src})
                    MATCH (b:User {id: n.dst})
                    MERGE (a)-[:FOLLOWS]->(b)
                    """, edges = edges_chunk
                )

            follows.clear()

            for member_chunk in split_to_chunks(memberships):
                result = session.run(
                    """
                    UNWIND $membership as n
                    MATCH (u:User {id: n.user})
                    MATCH (c:Circle {id: n.circle})
                    MERGE (u)-[:PART_OF]->(c)
                    """, membership = member_chunk
                )

            memberships.clear()


class PSQLIngestor:
    def __init__(self, username: str, password: str, host: str, port: int, dbname: str) -> None:
        import psycopg
        self.conn= psycopg.connect(dbname = dbname, user = username, password = password, host = host, port = port)
        self.setup_tables()

    def close(self):
        self.conn.close()

    def wipe(self):
        with self.conn.cursor() as cur:
            result = cur.execute(
                """
                truncate table
                node, ego, user_node, feature, node_features, circle, circle_member
cascade;
                """
            )
            self.conn.commit()

    def setup_tables(self):
        result = self.conn.execute(
            """
            create table if not exists node (
                node_id text primary key,
                node_type text not null,
                constraint node_type_check check (node_type in ('ego', 'user'))
            );

            create table if not exists ego (
                node_id text primary key references node(node_id) on delete cascade
            );
            
            create table if not exists user_node (
                node_id text primary key references node(node_id) on delete cascade,
                ego_id text not null references ego(node_id) on delete cascade
            );

            create table if not exists circle (
                circle_id text primary key,
                ego_id text not null references ego(node_id) on delete cascade,
                unique (ego_id, circle_id)
            );

            create table if not exists circle_member (
                circle_id text references circle(circle_id) on delete cascade,
                node_id text references node(node_id) on delete cascade,
                primary key (circle_id, node_id)
            );

            create table if not exists feature_group (
                group_id serial primary key,
                group_name text not null
            );

            create table if not exists feature_name (
                feature_id serial primary key,
                name text not null,
                group_id int not null references feature_group(group_id) on delete cascade,
                constraint feature_name_unique unique(group_id, name)
            );

            create table if not exists node_feature (
                node_id text not null references node(node_id) on delete cascade,
                feature_id int not null references feature_name(feature_id) on delete cascade,
                primary key (node_id, feature_id)
            );

            create table if not exists edge (
                src_id text not null references node(node_id) on delete cascade,
                dst_id text not null references node(node_id) on delete cascade,
                ego_id text not null references ego(node_id) on delete cascade,
                primary key (src_id, dst_id, ego_id)
            );
            """
        )
        self.conn.commit()

    def ingestEgoNetwork(
        self,
        ego_id: str,
        edges: list[tuple[str, str]],
        features: list[tuple[str, str]],
        node_features: dict[str, list[str]],
        ego_features: dict[str, list[str]],
        circles: dict[str, list[str]]
    ):
        cur = self.conn.cursor()

        cur.execute(
            """
            insert into node(node_id, node_type) values (%s, 'ego')
            on conflict do nothing
            """,
            (ego_id,)
        )

        cur.execute(
            """
            insert into ego(node_id) values (%s)
            on conflict do nothing
            """,
            (ego_id,)
        )
        self.conn.commit()

        users = [(uid, 'user') for uid in node_features.keys()]

        circs = [(cid, ego_id) for cid in circles.keys()]

        feature_groupnames = list({group for group, _ in features})

        for user_chunk in split_to_chunks(users):
            cur.executemany(
                """
                insert into node(node_id, node_type) values (%s, %s)
                on conflict do nothing
                """, user_chunk
            )
            self.conn.commit()

        for user_chunk in split_to_chunks(users):
            cur.executemany(
                """
                insert into user_node(node_id, ego_id) values (%s, %s)
                on conflict do nothing
                """, [(user, ego_id) for user, _ in user_chunk]
            )
            self.conn.commit()

        for circle_chunk in split_to_chunks(circs):
            cur.executemany(
                """
                insert into circle(circle_id, ego_id) values (%s, %s)
                on conflict do nothing
                """, circle_chunk
            )
            self.conn.commit()

        circs.clear()

        for feat_grname_chunk in split_to_chunks(feature_groupnames):
            cur.executemany(
                """
                insert into feature_group(group_name) values (%s)
                on conflict do nothing
                """, [(g,) for g in feat_grname_chunk]
            )
            self.conn.commit()

        group_map = dict(cur.execute("select group_name, group_id from feature_group").fetchall())

        for feature_chunk in split_to_chunks([(group_map[group], name) for group, name in features]):
            cur.executemany(
                """
                insert into feature_name (group_id, name) values (%s, %s)
                on conflict do nothing
                """, feature_chunk
            )
            self.conn.commit()

        feature_id_map = {(gid, name): fid for fid, gid, name in cur.execute("select feature_id, group_id, name from feature_name").fetchall()}

        node_feature_rows = []
        for node_id, feats in node_features.items():
            for name in feats:
                group_id = next(gid for (gid, fname) in feature_id_map if fname == name)
                feature_id = feature_id_map[(group_id, name)]
                node_feature_rows.append((node_id, feature_id))

        for ego, feats in ego_features.items():
            for name in feats:
                group_id = next(gid for (gid, fname) in feature_id_map if fname == name)
                feature_id = feature_id_map[(group_id, name)]
                node_feature_rows.append((ego, feature_id))

        for featmap_chunk in split_to_chunks(node_feature_rows):
            cur.executemany(
                """
                insert into node_feature(node_id, feature_id) values (%s, %s)
                on conflict do nothing
                """, featmap_chunk
            )
            self.conn.commit()

        feature_id_map.clear()
        group_map.clear()
        
        for edges_chunk in split_to_chunks(edges):
            cur.executemany(
                """
                insert into edge(src_id, dst_id, ego_id) values (%s, %s, %s)
                on conflict do nothing
                """, [(src, dst, ego_id) for src, dst in edges_chunk]
            )
            self.conn.commit()
