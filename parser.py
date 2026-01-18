from base64 import encode
from collections import defaultdict
from pathlib import Path

def getUids(path: Path) -> list[str]:
    uids: list[str] = []

    for file in path.iterdir():
        if file.suffix == '.edges':
            uids.append(file.stem)

    return uids

def parseEdges(path: Path) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            src, dst = line.strip().split()
            edges.append((src,dst))

    return edges

def parseFeatNames(path: Path) -> dict[int, str]:
    feature_names: dict[int, str] = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            idx, name = line.rstrip().split(" ", 1)
            feature_names[int(idx)] = name

    return feature_names

def mapNamesToFeats(path: Path, featNames: dict[int, str]) -> dict[str, list[str]]:
    features: dict[str, list[str]] = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip().split()
            nodeId = parts[0]

            active = [
                featNames[i] 
                for i, v in enumerate(parts[1:])
                if v == "1"
            ]

            features[nodeId] = active

    return features

def mapNamesToEgoFeats(path: Path, featNames: dict[int, str], egoId: str) -> dict[str, list[str]]:
    with open(path, "r", encoding = "utf-8") as f:
        values = f.readline().strip().split()

    return {egoId: [featNames[i] for i, v in enumerate(values) if v == '1']}

def parseCircles(path: Path) -> dict[str, list[str]]:
    circles: dict[str, list[str]] = defaultdict(list[str])
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            circle = parts[0]
            for node_id in parts[1:]:
                circles[circle].append(node_id)
    return circles

