#!/usr/bin/env python3
"""Seed an S3-compatible fixture server (RUSTFS): create the bucket and PUT
one object. Stdlib-only SigV4 (the same 20-line pattern the 015 environment
gate proved live); fixture tooling only — never product code.

Usage: seed_s3.py <endpoint> <access> <secret> <bucket> <key> <local-file>
"""
import datetime
import hashlib
import hmac
import sys
import urllib.error
import urllib.request

endpoint, access, secret, bucket, key, local = sys.argv[1:7]
region = "us-east-1"
host = endpoint.split("://", 1)[1]


def sign(k, m):
    return hmac.new(k, m.encode(), hashlib.sha256).digest()


def request(method, path, body=b"", ok_codes=(200,)):
    t = datetime.datetime.now(datetime.timezone.utc)
    amz, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
    payload = hashlib.sha256(body).hexdigest()
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
        data=body if body else None,
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
        if e.code not in ok_codes:
            sys.exit(f"{method} {path}: HTTP {e.code}: {e.read()[:300]}")


request("PUT", f"/{bucket}", ok_codes=(200, 409))  # 409 = already exists
with open(local, "rb") as fh:
    request("PUT", f"/{bucket}/{key}", body=fh.read())
print(f"seeded s3://{bucket}/{key}")
