import hashlib, json, pathlib, sys, zipfile
from importlib.metadata import PathDistribution

path = pathlib.Path(sys.argv[1])
try:
    with zipfile.ZipFile(path) as archive:
        tops = {name.split("/")[0] for name in archive.namelist()}
        infos = sorted(top for top in tops if top.endswith(".dist-info"))
        if len(infos) != 1:
            raise ValueError(f"it holds {len(infos)} .dist-info directories, not one")
        distribution = PathDistribution(zipfile.Path(archive, infos[0] + "/"))
        name = distribution.metadata["Name"]
        if not name:
            raise ValueError("its metadata names no distribution")
        found = distribution.entry_points.select(group="rdlt.connectors")
        entry_points = [[entry.name, entry.value] for entry in found]
    with path.open("rb") as wheel:
        sha256 = hashlib.file_digest(wheel, "sha256").hexdigest()
except Exception as error:
    print(json.dumps({"refused": str(error) or type(error).__name__}))
else:
    said = {"name": name, "uri": path.as_uri(), "sha256": sha256, "entry_points": entry_points}
    print(json.dumps(said))
