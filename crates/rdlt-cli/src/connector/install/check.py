import runpy, sys, sysconfig
for key in ("purelib", "platlib"):
    found = sysconfig.get_path(key)
    if found not in sys.path:
        sys.path.append(found)
sys.argv = ["pip", "check", "--disable-pip-version-check"]
runpy.run_module("pip", run_name="__main__", alter_sys=True)
