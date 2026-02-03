from typing import LiteralString

POSTGRES_SIMPLE: dict[str, LiteralString] = {
    "S1_nodes_by_type": """
        SELECT node_type, COUNT(*) FROM node GROUP BY node_type;
    """,
    "S2_users_per_ego": """
        SELECT ego_id, COUNT(*) FROM user_node GROUP BY ego_id;
    """,
    "S3_edges_per_ego": """
        SELECT ego_id, COUNT(*) FROM edge GROUP BY ego_id;
    """,
    "S4_circles_per_ego": """
        SELECT ego_id, COUNT(*) FROM circle GROUP BY ego_id;
    """,
    "S5_avg_features_per_node": """
        SELECT AVG(feature_count)
        FROM (
            SELECT node_id, COUNT(*) AS feature_count
            FROM node_feature
            GROUP BY node_id
        ) t;
    """,
    "S6_top_features": """
        SELECT fn.name, COUNT(*) AS usage_count
        FROM node_feature nf
        JOIN feature_name fn ON nf.feature_id = fn.feature_id
        GROUP BY fn.name
        ORDER BY usage_count DESC
        LIMIT 10;
    """
}

POSTGRES_COMPLEX: dict[str, LiteralString] = {
    "C1_avg_circle_size": """
        SELECT c.ego_id, AVG(member_count)
        FROM (
            SELECT circle_id, COUNT(*) AS member_count
            FROM circle_member
            GROUP BY circle_id
        ) cm
        JOIN circle c ON cm.circle_id = c.circle_id
        GROUP BY c.ego_id;
    """,
    "C2_high_degree_users": """
        SELECT src_id, ego_id, COUNT(*) AS degree
        FROM edge
        GROUP BY src_id, ego_id
        HAVING COUNT(*) > (
            SELECT AVG(cnt)
            FROM (
                SELECT COUNT(*) AS cnt
                FROM edge
                WHERE ego_id = edge.ego_id
                GROUP BY src_id
            ) t
        );
    """,
    "C3_feature_overlap": """
        SELECT cm1.circle_id, COUNT(*)
        FROM circle_member cm1
        JOIN circle_member cm2 
          ON cm1.circle_id = cm2.circle_id
         AND cm1.node_id < cm2.node_id
        JOIN node_feature nf1 ON cm1.node_id = nf1.node_id
        JOIN node_feature nf2 
          ON cm2.node_id = nf2.node_id
         AND nf1.feature_id = nf2.feature_id
        GROUP BY cm1.circle_id;
    """,
    "C4_triangle_proxy": """
        SELECT ego_id,
        COUNT(*) AS two_hop_paths,
        COUNT(DISTINCT src_id) AS distinct_sources,
        COUNT(*)::float / COUNT(DISTINCT src_id) AS closure_potential
        FROM (
            SELECT
                e1.ego_id,
                e1.src_id,
                e2.dst_id
            FROM edge e1
            JOIN edge e2
              ON e1.dst_id = e2.src_id
             AND e1.ego_id = e2.ego_id
        ) t
        GROUP BY ego_id;
    """
}
NEO4J_SIMPLE: dict[str, LiteralString]= {
    "S1_nodes_by_label": """
        MATCH (n)
        RETURN labels(n) AS labels, COUNT(*) AS count;
    """,

    "S2_users_per_ego": """
        MATCH (e:Ego)-[:FOLLOWS]->(u:User)
        RETURN e.id AS ego_id, COUNT(u) AS user_count;
    """,

    "S3_edges_per_ego": """
        MATCH (e:Ego)-[:FOLLOWS]->()
        RETURN e.id AS ego_id, COUNT(*) AS follows_count;
    """,

    "S4_circles_per_ego": """
        MATCH (e:Ego)-[:OWNS]->(c:Circle)
        RETURN e.id AS ego_id, COUNT(c) AS circle_count;
    """,

    "S5_avg_features_per_user": """
        MATCH (u:User)-[:HAS_FEAT]->(f:FeatName)
        WITH u, COUNT(f) AS feature_count
        RETURN AVG(feature_count) AS avg_features_per_user;
    """,

    "S6_top_features": """
        MATCH ()-[:HAS_FEAT]->(f:FeatName)
        RETURN f.name AS feature_name, COUNT(*) AS usage
        ORDER BY usage DESC
        LIMIT 10;
    """
}

NEO4J_COMPLEX: dict[str, LiteralString] = {
    "C1_avg_circle_size": """
        MATCH (e:Ego)-[:OWNS]->(c:Circle)<-[:PART_OF]-(u:User)
        WITH e, c, COUNT(u) AS member_count
        RETURN e.id AS ego_id, AVG(member_count) AS avg_circle_size;
    """,

    "C2_high_degree_users": """
        MATCH (u:User)-[:FOLLOWS]->()
        WITH u, COUNT(*) AS degree

        WITH collect({user: u, degree: degree}) AS users,
             avg(degree) AS avg_degree

        UNWIND users AS entry
        WITH entry, avg_degree
        WHERE entry.degree > avg_degree

        RETURN entry.user.id AS user_id,
       entry.degree AS degree;
    """,

    "C3_feature_overlap": """
        MATCH (c:Circle)<-[:PART_OF]-(u1:User),
              (c)<-[:PART_OF]-(u2:User),
              (u1)-[:HAS_FEAT]->(f:FeatName)<-[:HAS_FEAT]-(u2)
        WHERE u1.id < u2.id
        RETURN c.id AS circle_id,
               COUNT(DISTINCT f.name) AS shared_features;
    """,

    "C4_triangle_proxy": """
        MATCH (e:Ego)-[:FOLLOWS]->(u1:User)
              -[:FOLLOWS]->(u2:User)
              -[:FOLLOWS]->(u1)
        RETURN e.id AS ego_id, COUNT(*) AS triangle_count;
    """
}

