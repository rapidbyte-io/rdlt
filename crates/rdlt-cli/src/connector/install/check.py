import importlib.util, runpy, sys, sysconfig
environment = {"base": sys.argv[1], "platbase": sys.argv[1]}
for key in ("purelib", "platlib"):
    found = sysconfig.get_path(key, "venv", vars=environment)
    if found not in sys.path:
        sys.path.append(found)
if importlib.util.find_spec("pip") is None:
    sys.exit(100)
sys.argv = ["pip", "check", "--disable-pip-version-check"]
runpy.run_module("pip", run_name="__main__", alter_sys=True)
