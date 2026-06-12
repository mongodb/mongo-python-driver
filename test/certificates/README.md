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
conflicting requirements from Python's ssl module and macOS's SecTrust framework:

**MongoDB certs** — presented to MongoDB Enterprise, verified by Apple SecTrust on macOS.
No Authority Key Identifier (AKI) or Subject Key Identifier (SKI).  Adding AKI causes SecTrust to attempt OCSP revocation checks; because our
CA is not in the macOS system keychain, those checks fail with `CSSMERR_TP_CERT_SUSPENDED`.

**KMS certs** — presented by KMS mock servers, verified by Python's ssl module (OpenSSL).
Carry both AKI and SKI.  Python 3.13 requires AKI on non-root certs; Python 3.14 enables
`X509_V_FLAG_X509_STRICT` in `ssl.create_default_context()`, which requires SKI too.

| File | Subject | Signed by | Extensions | Purpose |
|---|---|---|---|---|
| `ca.pem` | `CN=Drivers Testing CA, ...` | Self (CA) | basicConstraints critical | Root CA for all test certs |
| `server.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN only | MongoDB server cert (key + cert) |
| `client.pem` | `CN=client, O=MDB, ...` | Drivers Testing CA | keyUsage, extKeyUsage | Client auth cert (key + cert) |
| `password_protected.pem` | Same as client | Drivers Testing CA | keyUsage, extKeyUsage | Client cert with AES-256 encrypted key |
| `crl.pem` | — | Drivers Testing CA | — | CRL revoking serial 1 (server.pem) |
| `server-kms.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN, AKI, SKI | KMS mock server cert (key + cert) |
| `wrong-host.pem` | `CN=wronghost.example.com` | Drivers Testing CA | SAN, AKI, SKI | KMS wrong-host test cert |
| `expired.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | SAN, AKI, SKI | KMS expired cert (validity 2000–2001) |
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

1. Python 3.13 / OpenSSL 3.x requires **AKI** on non-root certs.  The original 2019 certs had none.
2. Python 3.14 enables `X509_V_FLAG_X509_STRICT` in `ssl.create_default_context()`, which
   additionally requires **SKI** on non-root certs.

The CA cert intentionally omits SKI: adding SKI to the CA triggers macOS SecTrust OCSP
revocation checks on the MongoDB server startup path (MongoDB Enterprise on macOS uses Apple
SecTrust), causing ~67-second connection timeouts.  KMS connections use
`ssl.create_default_context()` but clear `ssl.VERIFY_X509_STRICT` on macOS so that the missing
CA SKI does not cause a verification failure.
