# rdlt-connector-greetings

The greetings connector, a source of one stream, `numbers`. Its certification is a test of each
clause: with `rdlt` installed, `pip install -e .` and `pytest` run them, and they pass as
written. Write the stream's reads in `src/rdlt_connector_greetings/__init__.py`, and keep them
passing.

To place it, build its wheel, with `uv build` or `pip wheel --no-deps -w dist .`, and install it
as a directory connector, an environment of its own: `rdlt connector install dist/<the wheel>`,
on Linux with a system Python, `--python /usr/bin/python3.N`, which the sandbox shows. A
pipeline then names it `io.example.greetings`.
