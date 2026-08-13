#!/usr/bin/env python3
"""Create a bucket on an S3-compatible fixture server (RUSTFS). Stdlib-only
SigV4 (the same pattern seed_s3.py uses); fixture tooling only, never product
code. Used to bring up the empty `lake` output bucket beside the seeded `raw`.

Usage: s3_bucket.py <endpoint> <access> <secret> <bucket>
"""
import datetime
import hashlib
import hmac
import sys
import urllib.error
import urllib.request

endpoint, access, secret, bucket = sys.argv[1:5]
region = "us-east-1"
host = endpoint.split("://", 1)[1]


def sign(k, m):
    return hmac.new(k, m.encode(), hashlib.sha256).digest()


def request(method, path, ok_codes=(200, 409)):
    t = datetime.datetime.now(datetime.timezone.utc)
    amz, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
    payload = hashlib.sha256(b"").hexdigest()
    creq = (
        f"{method}\n{path}\n\nhost:{host}\nx-amz-content-sha256:{payload}\n"
        f"x-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
    )
    scope = f"{ds}/{region}/s3/aws4_request"
    sts = (
        "AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n"
        + hashlib.sha256(creq.encode()).hexdigest()
    )
    sig = hmac.new(
        sign(sign(sign(sign(("AWS4" + secret).encode(), ds), region), "s3"), "aws4_request"),
        sts.encode(),
        hashlib.sha256,
    ).hexdigest()
    req = urllib.request.Request(
        endpoint + path,
        method=method,
        headers={
            "x-amz-date": amz,
            "x-amz-content-sha256": payload,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
            ),
        },
    )
    try:
        urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        if e.code not in ok_codes:  # 409 = bucket already exists
            sys.exit(f"{method} {path}: HTTP {e.code}: {e.read()[:300]}")


request("PUT", f"/{bucket}")
print(f"created s3://{bucket}")
