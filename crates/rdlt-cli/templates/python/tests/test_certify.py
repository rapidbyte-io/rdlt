"""The connector's certification: each clause a test of its own."""

from rdlt.testing import certify_source

from rdlt_connector_greetings import Greetings

# The kill clause stops a read after its second or third commit: one that ends sooner leaves the
# clause unobserved, which fails it, so the certification reads tens of thousands of rows.
test_certify = certify_source(Greetings, config={"rows": 50_000})
