#!/usr/bin/env python3
"""Print the total size, in bytes, of every object under an S3 prefix.

Used by a cell's `artifact_bytes_sh` to record what each arm actually WROTE,
next to how long it took. Two arms can move identical rows and leave behind
very different artifacts — rdlt wrote 210.0 MB where dlt wrote 73.7 MB for the
same million rows — and a timing comparison that does not say so is comparing
different work.

Standard library only, on purpose. The benchmark harness must not need a
Python environment provisioned for it, and boto3 would be a dependency of the
measurement rather than of anything measured. SigV4 is about forty lines.

    s3_prefix_bytes.py ENDPOINT BUCKET PREFIX ACCESS_KEY SECRET_KEY [REGION]

Prints one integer. Any failure exits non-zero with a message on stderr; the
harness records the size as ABSENT rather than zero, because "we could not
measure it" and "it wrote nothing" are different facts.
"""

import datetime
import hashlib
import hmac
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(secret: str, date: str, region: str) -> bytes:
    k = _sign(f"AWS4{secret}".encode(), date)
    k = _sign(k, region)
    k = _sign(k, "s3")
    return _sign(k, "aws4_request")


def _request(endpoint, bucket, query, access_key, secret_key, region):
    """One signed GET against the bucket, returning the response body."""
    url = urllib.parse.urlparse(endpoint)
    # Path-style addressing: the harness's object store is a local test server,
    # and path style is what it (and most S3-compatible servers) accept.
    canonical_uri = f"/{bucket}"
    host = url.netloc

    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    # Query parameters must be sorted by key for the canonical request.
    canonical_query = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(query.items())
    )
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [
            "GET",
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )

    scope = f"{date_stamp}/{region}/s3/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(secret_key, date_stamp, region), to_sign.encode(), hashlib.sha256
    ).hexdigest()

    request = urllib.request.Request(
        f"{endpoint.rstrip('/')}{canonical_uri}?{canonical_query}",
        headers={
            "Host": host,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def _parse(body: bytes):
    """Parse a ListObjectsV2 response, refusing anything carrying a DTD.

    `xml.etree` is the stdlib parser and this file stays stdlib-only, so the
    hardening is done here rather than by reaching for `defusedxml`: both
    entity-expansion ("billion laughs") and external-entity attacks require a
    DOCTYPE or ENTITY declaration, and a well-formed S3 listing has neither.
    Refusing one costs nothing legitimate and turns a hostile or compromised
    endpoint into a loud failure instead of a hang.

    The response is only as trustworthy as the endpoint the caller named, and
    that endpoint comes from a config file — so it is not automatically ours.
    """
    head = body[:4096].lstrip()
    lowered = head.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise RuntimeError("response carries a DTD, which a bucket listing never does — refusing")
    return ET.fromstring(body)


def total_bytes(endpoint, bucket, prefix, access_key, secret_key, region):
    """Sum Size across every page of ListObjectsV2."""
    total = 0
    seen = 0
    token = None
    while True:
        query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            query["continuation-token"] = token
        root = _parse(_request(endpoint, bucket, query, access_key, secret_key, region))
        for contents in root.findall(f"{S3_NS}Contents"):
            size = contents.find(f"{S3_NS}Size")
            if size is not None and size.text:
                total += int(size.text)
                seen += 1
        # Paginate rather than trusting one page: a 1M-row load writes more
        # than the 1000-key maximum, and a silent truncation here would
        # under-report exactly the arms that write the most.
        truncated = root.find(f"{S3_NS}IsTruncated")
        if truncated is None or truncated.text != "true":
            break
        token_el = root.find(f"{S3_NS}NextContinuationToken")
        if token_el is None or not token_el.text:
            raise RuntimeError("response says truncated but carries no continuation token")
        token = token_el.text
    if seen == 0:
        raise RuntimeError(f"no objects under s3://{bucket}/{prefix}")
    return total


def main(argv):
    if len(argv) not in (6, 7):
        print(__doc__, file=sys.stderr)
        return 2
    endpoint, bucket, prefix, access_key, secret_key = argv[1:6]
    region = argv[6] if len(argv) == 7 else "us-east-1"
    try:
        print(total_bytes(endpoint, bucket, prefix, access_key, secret_key, region))
    except (urllib.error.URLError, ET.ParseError, RuntimeError, OSError) as e:
        print(f"s3_prefix_bytes: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
