import json, os, sys, sysconfig
print(json.dumps({"version": list(sys.version_info[:3]), "executable": os.path.realpath(sys.executable), "stdlib": os.path.realpath(sysconfig.get_paths()["stdlib"])}))
