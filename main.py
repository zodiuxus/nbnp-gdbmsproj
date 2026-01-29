from pathlib import Path
import parser, ingestor
from os import remove, getenv 
from os.path import join, dirname
from alive_progress import alive_bar
from dotenv import load_dotenv


if __name__ == "__main__":
    if not load_dotenv(join(dirname(__file__), '.env')):
        print("Unable to get .env file. Is it present?")
        print("Must have the following variables:")
        print("""
            N4J_URL (including port)
            N4J_USER
            N4J_PW
            N4J_DB
            PG_URL
            PG_PORT
            PG_USER
            PG_PW
            PG_DB
        """)
        exit(0)

    print("Checking for dataset...")
    dataDir = Path("./gplus")
    if not dataDir.is_dir():
        print("Dataset directory not found!")
        zipfile = f"{dataDir}.tar.gz"
        if not Path(zipfile).exists():
            import requests
            print("Downloading dataset...")
            url = "https://snap.stanford.edu/data/gplus.tar.gz"
            with requests.get(url, stream = True) as r:
                with open(zipfile, 'wb') as f:
                    for chunk in r.iter_content(chunk_size = 8192):
                        f.write(chunk)
        print("Extracting dataset...")
        from shutil import unpack_archive
        unpack_archive(zipfile, format = "gztar")
        remove(zipfile)
    else:
        print("Found dataset!")

    uids = parser.getUids(dataDir)

    print("Connecting to Neo4J and PostgreSQL...")

    n4j_url = str(getenv("N4J_URL"))
    n4j_user = str(getenv("N4J_USER"))
    n4j_pw = str(getenv("N4J_PW"))
    n4j_db = str(getenv("N4J_DB"))
    
    pg_url = str(getenv("PG_URL"))
    pg_port = int(getenv("PG_PORT"))
    pg_user = str(getenv("PG_USER"))
    pg_pw = str(getenv("PG_PW"))
    pg_db = str(getenv("PG_DB"))

    n4ji = ingestor.Neo4JIngestor(n4j_url, n4j_user, n4j_pw, n4j_db)
    psql = ingestor.PSQLIngestor(pg_user, pg_pw, pg_url, pg_port, pg_db)

    print("Importing dataset to Neo4J and PostgreSQL...")
    with alive_bar(len(uids)) as bar:
        for uid in uids:
            edges = parser.parseEdges(dataDir / f"{uid}.edges")
            featNames = parser.parseFeatNames(dataDir / f"{uid}.featnames")
            userFeatures = parser.mapFeatsToUser(dataDir / f"{uid}.feat", featNames)
            egoFeatures = parser.mapFeatsToUser(dataDir / f"{uid}.egofeat", featNames, uid)
            circles = parser.parseCircles(dataDir / f"{uid}.circles")
            n4ji.ingestEgoNetwork(uid, edges, [(i, v) for i, v in featNames.values()], userFeatures, egoFeatures, circles)
            psql.ingestEgoNetwork(uid, edges, [(i, v) for i, v in featNames.values()], userFeatures, egoFeatures, circles)
            bar()
    n4ji.close()
    psql.close()

