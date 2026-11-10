import json, os, sys
print(json.dumps({"version": list(sys.version_info[:3]), "executable": os.path.realpath(sys.executable), "base": os.path.abspath(sys._base_executable), "stdlib": os.path.realpath(os.path.dirname(os.__file__))}))
