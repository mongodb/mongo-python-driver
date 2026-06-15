# /// script
# requires-python = ">=3.8"
# dependencies = ["cryptography>=44.0.0"]
# ///
"""Generate TLS test certificates for the PyMongo test suite.

Two classes of leaf cert are generated:

  MongoDB certs (server.pem, client.pem, password_protected.pem):
    No Authority Key Identifier (AKI) extension.  MongoDB Enterprise on macOS uses Apple SecTrust with
    kSecRevocationRequirePositiveResponse.  When AKI is present, SecTrust uses
    it to identify the issuer, then attempts OCSP.  Because our CA is not in
    the macOS system keychain on Evergreen driver CI hosts, OCSP fails and
    SecTrust returns CSSMERR_TP_CERT_SUSPENDED.  Without AKI, SecTrust cannot
    identify the issuer and skips the OCSP attempt.

  KMS certs (kms-server.pem, kms-wrong-host.pem, kms-expired.pem):
    Carry AKI in keyid form (keyIdentifier = SHA-1 of CA public key) and SKI.
    These certs are verified by Python's ssl module (OpenSSL), not by MongoDB
    Enterprise.  Python 3.13 requires AKI on non-root certs; Python 3.14
    additionally enables X509_V_FLAG_X509_STRICT which requires the keyid field
    within AKI (issuer/serial form is not sufficient).

The CA (ca.pem) carries basicConstraints (critical) and SKI.  SKI is needed
so that keyid-form AKI on the KMS leaf certs can reference the CA's public key.
macOS SecTrust OCSP is triggered by AKI on leaf certs, not by SKI on the CA:
since the MongoDB leaf certs (server.pem, client.pem) carry no AKI, SecTrust
cannot identify the issuer and skips the OCSP attempt — adding SKI to the CA
does not re-enable that check.

Usage:
    uv run gen-certs.py    # run from test/certificates/

Password for password_protected.pem: qwerty
"""
from __future__ import annotations

import datetime
import ipaddress
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

SCRIPT_DIR = Path(__file__).parent.resolve()
DAYS = 7300  # ~20 years
NOW = datetime.datetime.now(datetime.timezone.utc)
NOT_BEFORE = NOW - datetime.timedelta(days=1)
NOT_AFTER = NOW + datetime.timedelta(days=DAYS)


def make_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def key_pem(key, password=None) -> bytes:
    enc = BestAvailableEncryption(password) if password else NoEncryption()
    return key.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, enc)


def cert_pem(cert) -> bytes:
    return cert.public_bytes(Encoding.PEM)


def aki_from_ca(ca_key_pub) -> x509.AuthorityKeyIdentifier:
    # Keyid form: the keyIdentifier is the SHA-1 hash of the CA's public key
    # (same value as the CA's SKI).  OpenSSL 3.3+ strict mode requires this
    # form; issuer/serial form is not recognised as satisfying the AKI check.
    return x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key_pub)


def server_san() -> x509.SubjectAlternativeName:
    return x509.SubjectAlternativeName(
        [
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv6Address("::1")),
        ]
    )


CA_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COMMON_NAME, "Drivers Testing CA"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
    ]
)

SERVER_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
    ]
)

# Attribute order must be CN→OU→O→L→ST→C so that MongoDB's reversed-order
# x509 username string is "C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client"
CLIENT_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COMMON_NAME, "client"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MDB"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
    ]
)

TRUSTED_CA_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COMMON_NAME, "Trusted Kernel Test CA"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Kernel"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
    ]
)


# ---------------------------------------------------------------------------
# 0. Drivers Testing CA.
#    Has basicConstraints (critical, CA:TRUE) and SKI.  No keyUsage, no AKI,
#    no SAN.
#
#    keyUsage is intentionally omitted: on Windows Python 3.13, OpenSSL 3.3+
#    raises "certificate signature failure" when a CA cert has a critical
#    keyUsage with digital_signature=False, even though keyCertSign is set.
#    Python 3.14 strict mode only requires keyUsage to include keyCertSign
#    IF keyUsage is present — it does not require keyUsage to be present.
#
#    SKI is present so that KMS leaf certs can use keyid-form AKI.  OpenSSL
#    3.3+ strict mode requires the keyIdentifier field in AKI; issuer/serial
#    form (the alternative) is NOT recognised.  The CA's SKI does not trigger
#    macOS SecTrust OCSP checks: OCSP is triggered by AKI on LEAF certs
#    identifying an issuer, and our MongoDB leaf certs (server.pem, client.pem)
#    carry no AKI.
#
#    AKI is intentionally omitted from the CA: it is self-signed, so AKI
#    would be redundant and adding it could confuse some validators.
# ---------------------------------------------------------------------------
print("==> Generating Drivers Testing CA...")
ca_key = make_key()
ca_cert = (
    x509.CertificateBuilder()
    .subject_name(CA_NAME)
    .issuer_name(CA_NAME)
    .public_key(ca_key.public_key())
    .serial_number(480006)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
    .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "ca.pem").write_bytes(cert_pem(ca_cert))
print("    ca.pem written (subject:", ca_cert.subject.rfc4514_string(), ")")


# ---------------------------------------------------------------------------
# 1. Server certificate — serial 1, revoked in crl.pem for test_tlsCRLFile_support
#    No AKI: presented to MongoDB Enterprise (Apple SecTrust on macOS).
# ---------------------------------------------------------------------------
print("==> Generating server certificate (no AKI)...")
server_key = make_key()
server_cert = (
    x509.CertificateBuilder()
    .subject_name(SERVER_NAME)
    .issuer_name(CA_NAME)
    .public_key(server_key.public_key())
    .serial_number(1)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(server_san(), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "server.pem").write_bytes(key_pem(server_key) + cert_pem(server_cert))
print("    server.pem written")


# ---------------------------------------------------------------------------
# 1b. KMS server certificate — serial 5, with AKI.
#     Used by kms_failpoint_server.py (port 9003).  Verified by Python's ssl
#     module (OpenSSL), NOT by MongoDB Enterprise — so AKI is safe here and
#     is required for Python 3.13 / OpenSSL 3.x chain building.
# ---------------------------------------------------------------------------
print("==> Generating KMS server certificate (with AKI)...")
server_kms_key = make_key()
server_kms_cert = (
    x509.CertificateBuilder()
    .subject_name(SERVER_NAME)
    .issuer_name(CA_NAME)
    .public_key(server_kms_key.public_key())
    .serial_number(5)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(server_san(), critical=False)
    .add_extension(aki_from_ca(ca_key.public_key()), critical=False)
    .add_extension(
        x509.SubjectKeyIdentifier.from_public_key(server_kms_key.public_key()), critical=False
    )
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "kms-server.pem").write_bytes(key_pem(server_kms_key) + cert_pem(server_kms_cert))
print("    kms-server.pem written")


# ---------------------------------------------------------------------------
# 2. Client certificate — serial 2
#    No AKI: presented to MongoDB Enterprise during x509 auth.
# ---------------------------------------------------------------------------
print("==> Generating client certificate (no AKI)...")
client_key = make_key()
client_cert = (
    x509.CertificateBuilder()
    .subject_name(CLIENT_NAME)
    .issuer_name(CA_NAME)
    .public_key(client_key.public_key())
    .serial_number(2)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=False,
    )
    .add_extension(
        x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
        critical=False,
    )
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "client.pem").write_bytes(key_pem(client_key) + cert_pem(client_cert))
print("    client.pem written")


# ---------------------------------------------------------------------------
# 3. Password-protected client certificate (same cert, encrypted key)
# ---------------------------------------------------------------------------
print("==> Generating password-protected client certificate...")
(SCRIPT_DIR / "password_protected.pem").write_bytes(
    key_pem(client_key, password=b"qwerty") + cert_pem(client_cert)
)
print("    password_protected.pem written (password: qwerty)")


# ---------------------------------------------------------------------------
# 4. CRL — revokes the server cert (serial 1) for test_tlsCRLFile_support
# ---------------------------------------------------------------------------
print("==> Generating CRL...")
crl = (
    x509.CertificateRevocationListBuilder()
    .issuer_name(CA_NAME)
    .last_update(NOW)
    .next_update(NOW + datetime.timedelta(days=DAYS))
    .add_revoked_certificate(
        x509.RevokedCertificateBuilder().serial_number(1).revocation_date(NOW).build()
    )
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "crl.pem").write_bytes(crl.public_bytes(Encoding.PEM))
print("    crl.pem written")


# ---------------------------------------------------------------------------
# 5. Wrong-host certificate (serial 3) — used in KMS TLS tests (with AKI)
# ---------------------------------------------------------------------------
print("==> Generating wrong-host certificate (with AKI)...")
wrong_host_key = make_key()
wrong_host_cert = (
    x509.CertificateBuilder()
    .subject_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "wronghost.example.com"),
                x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            ]
        )
    )
    .issuer_name(CA_NAME)
    .public_key(wrong_host_key.public_key())
    .serial_number(3)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(
        x509.SubjectAlternativeName([x509.DNSName("wronghost.example.com")]),
        critical=False,
    )
    .add_extension(aki_from_ca(ca_key.public_key()), critical=False)
    .add_extension(
        x509.SubjectKeyIdentifier.from_public_key(wrong_host_key.public_key()), critical=False
    )
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "kms-wrong-host.pem").write_bytes(key_pem(wrong_host_key) + cert_pem(wrong_host_cert))
print("    kms-wrong-host.pem written (SAN: wronghost.example.com)")


# ---------------------------------------------------------------------------
# 6. Expired certificate (serial 4) — used in KMS TLS tests (with AKI)
# ---------------------------------------------------------------------------
print("==> Generating expired certificate (with AKI)...")
expired_key = make_key()
expired_cert = (
    x509.CertificateBuilder()
    .subject_name(SERVER_NAME)
    .issuer_name(CA_NAME)
    .public_key(expired_key.public_key())
    .serial_number(4)
    .not_valid_before(datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
    .not_valid_after(datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc))
    .add_extension(server_san(), critical=False)
    .add_extension(aki_from_ca(ca_key.public_key()), critical=False)
    .add_extension(
        x509.SubjectKeyIdentifier.from_public_key(expired_key.public_key()), critical=False
    )
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "kms-expired.pem").write_bytes(key_pem(expired_key) + cert_pem(expired_cert))
print("    kms-expired.pem written (expired 2001-01-01)")


# ---------------------------------------------------------------------------
# 7. Trusted Kernel Test CA — separate CA used in CA-bundle tests only.
#    This is an independent CA unrelated to the main Drivers Testing CA.
# ---------------------------------------------------------------------------
print("==> Generating Trusted Kernel Test CA...")
trusted_ca_key = make_key()
trusted_ca_cert = (
    x509.CertificateBuilder()
    .subject_name(TRUSTED_CA_NAME)
    .issuer_name(TRUSTED_CA_NAME)
    .public_key(trusted_ca_key.public_key())
    .serial_number(200)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
    .add_extension(
        x509.KeyUsage(
            digital_signature=False,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=True,
            crl_sign=True,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )
    .sign(trusted_ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "trusted-ca.pem").write_bytes(cert_pem(trusted_ca_cert))
print("    trusted-ca.pem written")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
print()
print("==> Verifying cert properties...")

import subprocess


def cert_text(path: Path) -> str:
    return subprocess.check_output(
        ["openssl", "x509", "-noout", "-text", "-in", str(path)],
        stderr=subprocess.DEVNULL,
    ).decode()


errors = 0

# CA cert must have critical basicConstraints and SKI; must NOT have keyUsage, AKI, or SAN.
ca_text = cert_text(SCRIPT_DIR / "ca.pem")
ca_errors = 0
if "Basic Constraints: critical" not in ca_text:
    print(
        "    ca.pem: ERROR — basicConstraints not critical (required by Python 3.14 strict mode)",
        file=sys.stderr,
    )
    ca_errors += 1
if "Subject Key Identifier" not in ca_text:
    print(
        "    ca.pem: ERROR — missing SKI (required for keyid-form AKI on KMS leaf certs)",
        file=sys.stderr,
    )
    ca_errors += 1
for ext in ("Key Usage", "Authority Key Identifier", "Subject Alternative Name"):
    if ext in ca_text:
        print(
            f"    ca.pem: ERROR — has {ext} (would cause issues on Windows or macOS)",
            file=sys.stderr,
        )
        ca_errors += 1
if ca_errors:
    errors += ca_errors
else:
    print("    ca.pem: OK (has SKI, no keyUsage/AKI/SAN)")

# MongoDB certs must NOT have AKI.
for name in ("server.pem", "client.pem"):
    text = cert_text(SCRIPT_DIR / name)
    if "Authority Key Identifier" in text:
        print(
            f"    {name}: ERROR — has AKI (would cause CSSMERR_TP_CERT_SUSPENDED on macOS)",
            file=sys.stderr,
        )
        errors += 1
    else:
        print(f"    {name}: OK (no AKI)")

# KMS certs MUST have keyid-form AKI and SKI.
for name in ("kms-server.pem", "kms-wrong-host.pem", "kms-expired.pem"):
    text = cert_text(SCRIPT_DIR / name)
    cert_errors = 0
    if "Authority Key Identifier" not in text:
        print(f"    {name}: ERROR — missing AKI (required for Python 3.13+)", file=sys.stderr)
        cert_errors += 1
    elif "keyid:" not in text.lower() and "Key Identifier" not in text:
        print(
            f"    {name}: ERROR — AKI missing keyIdentifier (OpenSSL 3.3+ strict mode requires keyid form)",
            file=sys.stderr,
        )
        cert_errors += 1
    if "Subject Key Identifier" not in text:
        print(f"    {name}: ERROR — missing SKI (required for Python 3.14+)", file=sys.stderr)
        cert_errors += 1
    if cert_errors:
        errors += cert_errors
    else:
        print(f"    {name}: OK (has keyid-form AKI + SKI)")

if errors:
    sys.exit(1)

print()
print("Done. All certificates regenerated.")
