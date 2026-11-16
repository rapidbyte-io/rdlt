import json, os, shlex, sys

from pip._internal.index.collector import LinkCollector
from pip._internal.index.package_finder import PackageFinder
from pip._internal.models.search_scope import SearchScope
from pip._internal.models.selection_prefs import SelectionPreferences
from pip._internal.network.session import PipSession
from pip._internal.req.constructors import install_req_from_parsed_requirement
from pip._internal.req.req_file import break_args_options, parse_requirements

lines = json.load(open(sys.argv[1]))
session = PipSession()
read = []
for index, line in enumerate(lines):
    args, options = break_args_options(line)
    path = os.path.join(sys.argv[2], f"{index}.txt")
    with open(path, "w") as file:
        file.write(line + "\n")
    scope = SearchScope.create(find_links=[], index_urls=[], no_index=False)
    collector = LinkCollector(session=session, search_scope=scope)
    preferences = SelectionPreferences(allow_yanked=False)
    finder = PackageFinder.create(link_collector=collector, selection_prefs=preferences)
    requirements = []
    for parsed in parse_requirements(path, session, finder=finder):
        installing = install_req_from_parsed_requirement(parsed)
        req, link = installing.req, installing.link
        requirements.append({
            "name": req.name if req else None,
            "extras": sorted(req.extras) if req else None,
            "specifier": str(req.specifier) if req else None,
            "url": req.url if req else None,
            "marked": installing.markers is not None,
            "link": link.url if link else None,
            "wheel": link.is_wheel if link else None,
            "file": link.file_path if link and link.is_file else None,
            "options": parsed.options or {},
        })
    read.append({
        "args": args,
        "options": shlex.split(options),
        "requirements": requirements,
        "find_links": list(finder.find_links),
        "index_urls": list(finder.index_urls),
        "no_index": finder.search_scope.no_index,
        "no_binary": sorted(finder.format_control.no_binary),
        "only_binary": sorted(finder.format_control.only_binary),
    })
json.dump(read, sys.stdout)
