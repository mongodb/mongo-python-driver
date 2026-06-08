#!/usr/bin/env python3
"""Generate TLS test certificates for the PyMongo test suite.

Certificates include AKI on leaf certs (required by Python 3.13 / OpenSSL 3.x
chain building) but deliberately omit SKI on the CA cert.  Without an explicit
SKI on the CA, macOS SecTrust cannot perform keyid-based chain lookup and
therefore does not trigger its hard-fail OCSP check, which was causing
CSSMERR_TP_CERT_SUSPENDED errors during MongoDB replica-set inter-node TLS.

Usage:
    pip install cryptography
    python gen-certs.py    # run from test/certificates/

Password for password_protected.pem: qwerty
"""
from __future__ import annotations

import datetime
import ipaddress
import sys
from pathlib import Path

try:
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
except ImportError:
    sys.exit("cryptography package is required: pip install cryptography")

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


def aki_from_ca(ca_key) -> x509.AuthorityKeyIdentifier:
    # Derives keyid from the CA's public key directly — no SKI extension needed
    # on the CA cert.  Python 3.13 / OpenSSL 3.x require AKI to be present on
    # leaf certs; the keyid form satisfies that without requiring CA SKI.
    return x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key())


def server_san() -> x509.SubjectAlternativeName:
    return x509.SubjectAlternativeName(
        [
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv6Address("::1")),
        ]
    )


# Canonical names — kept stable so tests that hard-code DN strings keep passing.
CA_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
        x509.NameAttribute(NameOID.COMMON_NAME, "Drivers Testing CA"),
    ]
)

SERVER_NAME = x509.Name(
    [
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ]
)

# Attribute order must be CN→OU→O→L→ST→C so that MongoDB's reversed-order
# x509 username string is "C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client"
# (see MONGODB_X509_USERNAME in test/test_ssl.py).
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
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Kernel"),
        x509.NameAttribute(NameOID.COMMON_NAME, "Trusted Kernel Test CA"),
    ]
)


# ---------------------------------------------------------------------------
# 1. Drivers Testing CA
# ---------------------------------------------------------------------------
print("==> Generating Drivers Testing CA...")
ca_key = make_key()
ca_cert = (
    x509.CertificateBuilder()
    .subject_name(CA_NAME)
    .issuer_name(CA_NAME)
    .public_key(ca_key.public_key())
    .serial_number(100)
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    # basicConstraints without critical flag, no SKI — matches old x509gen CA
    # structure.  Omitting SKI prevents macOS SecTrust from resolving the CA
    # via AKI keyid, so it skips OCSP revocation checking for inter-node TLS.
    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "ca.pem").write_bytes(cert_pem(ca_cert))
print("    ca.pem written")


# ---------------------------------------------------------------------------
# 2. Server certificate — serial 1, revoked in crl.pem for test_tlsCRLFile_support
# ---------------------------------------------------------------------------
print("==> Generating server certificate...")
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
    .add_extension(aki_from_ca(ca_key), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "server.pem").write_bytes(key_pem(server_key) + cert_pem(server_cert))
print("    server.pem written")


# ---------------------------------------------------------------------------
# 3. Client certificate — serial 2
# ---------------------------------------------------------------------------
print("==> Generating client certificate...")
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
    .add_extension(aki_from_ca(ca_key), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "client.pem").write_bytes(key_pem(client_key) + cert_pem(client_cert))
print("    client.pem written")


# ---------------------------------------------------------------------------
# 4. Password-protected client certificate (same cert, encrypted key)
# ---------------------------------------------------------------------------
print("==> Generating password-protected client certificate...")
(SCRIPT_DIR / "password_protected.pem").write_bytes(
    key_pem(client_key, password=b"qwerty") + cert_pem(client_cert)
)
print("    password_protected.pem written (password: qwerty)")


# ---------------------------------------------------------------------------
# 5. CRL — revokes the server cert (serial 1) for test_tlsCRLFile_support
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
# 6. Wrong-host certificate (serial 3) — used in KMS TLS tests
# ---------------------------------------------------------------------------
print("==> Generating wrong-host certificate...")
wrong_host_key = make_key()
wrong_host_cert = (
    x509.CertificateBuilder()
    .subject_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "New York"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "New York City"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MongoDB"),
                x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Drivers"),
                x509.NameAttribute(NameOID.COMMON_NAME, "wronghost.example.com"),
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
    .add_extension(aki_from_ca(ca_key), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "wrong-host.pem").write_bytes(key_pem(wrong_host_key) + cert_pem(wrong_host_cert))
print("    wrong-host.pem written (SAN: wronghost.example.com)")


# ---------------------------------------------------------------------------
# 7. Expired certificate (serial 4) — used in KMS TLS tests
# ---------------------------------------------------------------------------
print("==> Generating expired certificate...")
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
    .add_extension(aki_from_ca(ca_key), critical=False)
    .sign(ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "expired.pem").write_bytes(key_pem(expired_key) + cert_pem(expired_cert))
print("    expired.pem written (expired 2001-01-01)")


# ---------------------------------------------------------------------------
# 8. Trusted Kernel Test CA — separate CA, used in CA-bundle tests
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
    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=False)
    .sign(trusted_ca_key, hashes.SHA256())
)
(SCRIPT_DIR / "trusted-ca.pem").write_bytes(cert_pem(trusted_ca_cert))
print("    trusted-ca.pem written")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
print()
print("==> Verifying AKI on leaf certs and no SKI on CA...")

import subprocess


def cert_extensions(path: Path) -> str:
    return subprocess.check_output(
        ["openssl", "x509", "-noout", "-text", "-in", str(path)],
        stderr=subprocess.DEVNULL,
    ).decode()


errors = 0
for name in ("server.pem", "client.pem", "wrong-host.pem", "expired.pem"):
    text = cert_extensions(SCRIPT_DIR / name)
    has_aki = "Authority Key Identifier" in text
    has_ski = "Subject Key Identifier" in text
    if not has_aki:
        print(f"    {name}: MISSING AKI", file=sys.stderr)
        errors += 1
    elif has_ski:
        print(f"    {name}: OK (AKI present, but unexpected SKI also present)")
    else:
        print(f"    {name}: OK")

ca_text = cert_extensions(SCRIPT_DIR / "ca.pem")
if "Subject Key Identifier" in ca_text:
    print("    ca.pem: UNEXPECTED SKI — OpenSSL auto-added it", file=sys.stderr)
    errors += 1
else:
    print("    ca.pem: OK (no SKI)")

if errors:
    sys.exit(1)

print()
print("Done. All certificates regenerated.")
