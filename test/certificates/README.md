# Test TLS Certificates

These certificates are used by the PyMongo test suite for TLS/SSL integration tests.

## Regenerating certificates

Run the generation script from this directory:

```bash
bash gen-certs.sh
```

**Prerequisites:** OpenSSL 1.1+ or LibreSSL 3+

## Certificate details

| File | Subject | Signed by | Purpose |
|---|---|---|---|
| `ca.pem` | `CN=Drivers Testing CA, ...` | Self (CA) | Root CA for test certs |
| `server.pem` | `CN=localhost, ...` + SAN | Drivers Testing CA | MongoDB server cert (key + cert) |
| `client.pem` | `CN=client, O=MDB, ...` | Drivers Testing CA | Client auth cert (key + cert) |
| `password_protected.pem` | Same as client | Drivers Testing CA | Client cert with AES-256 encrypted key |
| `crl.pem` | — | Drivers Testing CA | Empty Certificate Revocation List |
| `trusted-ca.pem` | `CN=Trusted Kernel Test CA, OU=Kernel, ...` | Self (CA) | Separate CA for bundle tests |

**Password** for `password_protected.pem`: `qwerty`

## Important constraints

The following values are hardcoded in tests and **must not change**:

- Client cert subject: `C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client`
  (used as the MongoDB X.509 username in `test/test_ssl.py`)
- Server cert SAN: `DNS:localhost, IP:127.0.0.1, IP:::1`
- The `server` hostname alias for `127.0.0.1` must be present in `/etc/hosts` for SSL tests to pass
  (added automatically by `.evergreen/scripts/setup-system.sh`)

## Background

Certificates were regenerated to add the **Authority Key Identifier (AKI)** extension, which Python 3.13 requires for TLS certificate chain validation (PYTHON-5040). Prior to regeneration, certs were missing AKI, causing `ssl.SSLCertVerificationError: Missing Authority Key Identifier` on macOS and Windows with Python 3.13.
