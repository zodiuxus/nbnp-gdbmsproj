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

def parseFeatNames(path: Path) -> dict[int, tuple[str, str]]:
    """
    Returns a dict of features in the following format:
    {feature_id: (feature_group_name, feature_name)}
    """
    feature_names: dict[int, tuple[str, str]] = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            idx, name = line.rstrip().split(" ", 1)
            name = name.split(':')
            feature_names[int(idx)] = (name[0], name[1])

    return feature_names

def mapFeatsToUser(path: Path, feat_names: dict[int, tuple[str, str]], ego_id: str = "") -> dict[str, list[str]]:
    """
    Returns a dict of feature names
    mapped to a user in the following format:
    {userId: [list_of_associated_features]}
    """
    features: dict[str, list[str]] = {}

    with open(path, "r", encoding="utf-8") as f:
        if ego_id == "":
            for line in f:
                parts = line.rstrip().split()
                node_id = parts[0]

                active = [
                    feat_names[i][1]
                    for i, v in enumerate(parts[1:])
                    if v == "1"
                ]

                features[node_id] = active
        else:
            values = f.readline().strip().split()
            features[ego_id] = [feat_names[i][1] for i, v in enumerate(values) if v=='1']

    return features

def parseCircles(path: Path) -> dict[str, list[str]]:
    circles: dict[str, list[str]] = defaultdict(list[str])
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            circle = parts[0]
            for node_id in parts[1:]:
                circles[circle].append(node_id)
    return circles

