from pathlib import Path
import parser, ingestor
from alive_progress import alive_bar

if __name__ == "__main__":
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
    else:
        print("Found!")

    uids = parser.getUids(dataDir)
    n4ji = ingestor.Neo4JIngestor()

    with alive_bar(len(uids)) as bar:
        for uid in uids:
            edges = parser.parseEdges(dataDir / f"{uid}.edges")
            featNames = parser.parseFeatNames(dataDir / f"{uid}.featnames")
            userFeatures = parser.mapNamesToFeats(dataDir / f"{uid}.feat", featNames)
            egoFeatures = parser.mapNamesToEgoFeats(dataDir / f"{uid}.egofeat", featNames, uid)
            circles = parser.parseCircles(dataDir / f"{uid}.circles")
            n4ji.ingestEgoNetwork(uid, edges, userFeatures, egoFeatures, circles)
            bar()
    n4ji.close()
