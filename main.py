from pathlib import Path
import parser, ingestor, plots, benchmark
from os import remove, getenv 
from os.path import join, dirname
from alive_progress import alive_bar
from dotenv import load_dotenv
import argparse

def parse_args():
    parser = argparse.ArgumentParser(
        description="Graph vs Relational DB Pipeline"
    )

    parser.add_argument(
        "--segments",
        nargs="+",
        required=True,
        choices=["data-download", "data-import", "metrics"],
        help="Pipeline segments to run (choose at least one)"
    )

    parser.add_argument(
        "--db",
        choices=["p", "n", "b"],
        default="b",
        help="Database target: p=Postgres, n=Neo4j, b=both (default)"
    )

    return parser.parse_args()

def check_dataset(data_dir: Path):
    print("Checking for dataset...")
    if not data_dir.is_dir():
        print("Dataset directory not found!")
        zipfile = f"{data_dir}.tar.gz"
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

def import_data(db, data_dir: Path):
    uids = parser.getUids(data_dir)

    print("Importing dataset to Neo4J and PostgreSQL...")
    with alive_bar(len(uids)) as bar:
        for uid in uids:
            edges = parser.parseEdges(data_dir / f"{uid}.edges")
            featNames = parser.parseFeatNames(data_dir / f"{uid}.featnames")
            userFeatures = parser.mapFeatsToUser(data_dir / f"{uid}.feat", featNames)
            egoFeatures = parser.mapFeatsToUser(data_dir / f"{uid}.egofeat", featNames, uid)
            circles = parser.parseCircles(data_dir / f"{uid}.circles")
            db.ingestEgoNetwork(uid, edges, [(i, v) for i, v in featNames.values()], userFeatures, egoFeatures, circles)
            bar()

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

    args = parse_args()
    if not args.segments:
        print("No segments argument provided")
        exit()

    data_dir = Path("./gplus")

    if "data-download" in args.segments:
        check_dataset(data_dir)

    n4ji = None
    psql = None

    if "data-import" in args.segments:
        print("Connecting to database...")

        if args.db in ("p", "b"):
            pg_url = str(getenv("PG_URL"))
            pg_port = int(getenv("PG_PORT"))
            pg_user = str(getenv("PG_USER"))
            pg_pw = str(getenv("PG_PW"))
            pg_db = str(getenv("PG_DB"))
            psql = ingestor.PSQLIngestor(pg_user, pg_pw, pg_url, pg_port, pg_db)
            import_data(psql, data_dir)

        if args.db in ("n", "b"):
            n4j_url = str(getenv("N4J_URL"))
            n4j_user = str(getenv("N4J_USER"))
            n4j_pw = str(getenv("N4J_PW"))
            n4j_db = str(getenv("N4J_DB"))
            
            n4ji = ingestor.Neo4JIngestor(n4j_url, n4j_user, n4j_pw, n4j_db)
            import_data(n4ji, data_dir)
        
    if "metrics" in args.segments:
        print("Running metrics on database...")
        if args.db in ("p", "b"):
            if psql is None:
                pg_url = str(getenv("PG_URL"))
                pg_port = int(getenv("PG_PORT"))
                pg_user = str(getenv("PG_USER"))
                pg_pw = str(getenv("PG_PW"))
                pg_db = str(getenv("PG_DB"))
                psql = ingestor.PSQLIngestor(pg_user, pg_pw, pg_url, pg_port, pg_db)

        if args.db in ("n", "b"):
            if n4ji is None:
                n4j_url = str(getenv("N4J_URL"))
                n4j_user = str(getenv("N4J_USER"))
                n4j_pw = str(getenv("N4J_PW"))
                n4j_db = str(getenv("N4J_DB"))
                n4ji = ingestor.Neo4JIngestor(n4j_url, n4j_user, n4j_pw, n4j_db)

        benchmark.run_metrics(psql, n4ji)
        plots.plot_metrics() 

    if n4ji is not None:
        n4ji.close()

    if psql is not None:
        psql.close()

