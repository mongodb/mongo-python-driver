# Test TLS Certificates

These certificates are used by the PyMongo test suite for TLS/SSL integration tests.

## Regenerating certificates

Run the generation script from this directory:

```bash
uv run gen-certs.py
```

**Prerequisites:** Python 3 and [uv](https://docs.astral.sh/uv/). The script declares its own dependency on `cryptography` via PEP 723 inline metadata, so `uv` installs it automatically.

## Certificate details

Two classes of leaf certificate are generated, with different extension profiles to satisfy
conflicting requirements from Python's ssl module and macOS's SecTrust framework.
AKI (Authority Key Identifier) and SKI (Subject Key Identifier) are X.509 extensions that
identify the key used to sign a certificate and the certificate's own key, respectively.

**MongoDB certs** — presented to MongoDB Enterprise, verified by Apple SecTrust on macOS.
No AKI or SKI. Adding AKI triggers SecTrust OCSP checks; our CA has no OCSP responder, so those
fail with `CSSMERR_TP_CERT_SUSPENDED`. See Background below.

**KMS certs** — presented by KMS mock servers, verified by Python's ssl module (OpenSSL).
Carry both AKI (keyid form) and SKI. Python 3.13 requires AKI on non-root certs; Python 3.14
sets `X509_V_FLAG_X509_STRICT` in `ssl.create_default_context()`, which additionally requires SKI
and critical `keyUsage` on CA certs.

| File | Subject | Signed by | Extensions | Purpose |
|---|---|---|---|---|
| `ca.pem` | `CN=Drivers Testing CA, ...` | Self (CA) | basicConstraints critical, keyUsage critical, SKI | Root CA for all test certs |
| `server.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN only | MongoDB server cert (key + cert) |
| `client.pem` | `CN=client, O=MDB, ...` | Drivers Testing CA | keyUsage, extKeyUsage | Client auth cert (key + cert) |
| `password_protected.pem` | Same as client | Drivers Testing CA | keyUsage, extKeyUsage | Client cert with AES-256 encrypted key |
| `crl.pem` | — | Drivers Testing CA | — | CRL revoking serial 1 (server.pem) |
| `kms-server.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN, AKI, SKI | KMS mock server cert (key + cert) |
| `kms-wrong-host.pem` | `CN=wronghost.example.com` | Drivers Testing CA | SAN, AKI, SKI | KMS wrong-host test cert |
| `kms-expired.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN, AKI, SKI | KMS expired cert (validity 2000–2001) |
| `trusted-ca.pem` | `CN=Trusted Kernel Test CA, ...` | Self (CA) | basicConstraints critical, keyUsage critical | Separate CA for CA-bundle tests |

**Password** for `password_protected.pem`: `qwerty`

## Important constraints

The following values are hardcoded in tests and **must not change**:

- Client cert subject: `C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client`
  (used as the MongoDB X.509 username in `test/test_ssl.py`)
- Server cert SAN: `DNS:localhost, IP:127.0.0.1, IP:::1`
- The `server` hostname alias for `127.0.0.1` must be present in `/etc/hosts` for SSL tests to pass
  (added automatically by `.evergreen/scripts/setup-system.sh`)

## Background

Certificates were regenerated for PYTHON-5040 to fix `ssl.SSLCertVerificationError` failures on
macOS and Windows with Python 3.13+.  The root causes were:

1. Python 3.13 enables `X509_V_FLAG_X509_STRICT` in `ssl.create_default_context()`, which
   requires **AKI** on non-root certs.  The KMS mock-server connection (`http_post`) used
   `create_default_context()`, so the original 2019 KMS certs (no AKI) started failing.
2. Python 3.14 sets `ssl.VERIFY_X509_STRICT` (OpenSSL's `X509_V_FLAG_X509_STRICT`) in
   `ssl.create_default_context()`, which additionally requires **SKI** on non-root certs and
   **keyUsage** on CA certs.

MongoDB certs carry no AKI: AKI on any leaf cert triggers Apple SecTrust OCSP checks, which fail
(`CSSMERR_TP_CERT_SUSPENDED`) because our CA has no OCSP responder.  MongoDB Enterprise on macOS
uses `kSecRevocationRequirePositiveResponse`, so even without AKI the server startup fails; that is
why `.evergreen/scripts/run_server.py` passes `--tls-allow-invalid-certificates` on macOS.  The CA
carries SKI (but no AKI), which enables keyid-form AKI on KMS leaf certs without re-triggering
macOS OCSP.  As long as `pymongo.ssl_support.get_ssl_context` uses `PROTOCOL_SSLv23` (not
`create_default_context`), `X509_V_FLAG_X509_STRICT` is not set and no AKI is required on MongoDB
leaf certs.

KMS connections use `ssl.create_default_context()`, which sets `X509_V_FLAG_X509_STRICT`.
OpenSSL 3.3+ strict mode requires the `keyIdentifier` field within AKI; issuer/serial form is not
sufficient, so KMS leaf certs use keyid-form AKI derived from the CA's SKI.

> **If the driver is changed to use `ssl.create_default_context()` for MongoDB connections**, the
> MongoDB certs will need AKI and SKI.  Adding AKI will re-trigger macOS SecTrust OCSP failures;
> to resolve that, either add `--tls-allow-invalid-certificates` to the server startup in
> `run_server.py`, or install the test CA into the macOS system keychain in the CI setup.
