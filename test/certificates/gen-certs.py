# /// script
# requires-python = ">=3.8"
# dependencies = ["cryptography>=44.0.0"]
# ///
"""Generate TLS test certificates for the PyMongo test suite.

See README.md in this directory for background on the certificate design.

Usage:
    uv run gen-certs.py    # run from test/certificates/

Password for password_protected.pem: qwerty
"""

from __future__ import annotations

import datetime
import ipaddress
import subprocess
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes
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

CA_KEY_USAGE = x509.KeyUsage(
    digital_signature=False,
    content_commitment=False,
    key_encipherment=False,
    data_encipherment=False,
    key_agreement=False,
    key_cert_sign=True,
    crl_sign=True,
    encipher_only=False,
    decipher_only=False,
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


def cert_text(path: Path) -> str:
    return subprocess.check_output(
        ["openssl", "x509", "-noout", "-text", "-in", str(path)],
        stderr=subprocess.DEVNULL,
    ).decode()


def gen_ca() -> rsa.RSAPrivateKey:
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
        .add_extension(CA_KEY_USAGE, critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    (SCRIPT_DIR / "ca.pem").write_bytes(cert_pem(ca_cert))
    print("    ca.pem written (subject:", ca_cert.subject.rfc4514_string(), ")")
    return ca_key


def gen_server(ca_key: rsa.RSAPrivateKey) -> None:
    # Serial 1 is revoked in crl.pem for test_tlsCRLFile_support. No AKI (see README.md).
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


def gen_kms_server(ca_key: rsa.RSAPrivateKey) -> None:
    # Serial 5, with AKI + SKI (see README.md). Used by kms_failpoint_server.py (port 9003).
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


def gen_client(ca_key: rsa.RSAPrivateKey) -> None:
    # Serial 2, no AKI (see README.md).
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

    print("==> Generating password-protected client certificate...")
    (SCRIPT_DIR / "password_protected.pem").write_bytes(
        key_pem(client_key, password=b"qwerty") + cert_pem(client_cert)
    )
    print("    password_protected.pem written (password: qwerty)")


def gen_crl(ca_key: rsa.RSAPrivateKey) -> None:
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


def gen_kms_wrong_host(ca_key: rsa.RSAPrivateKey) -> None:
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
    (SCRIPT_DIR / "kms-wrong-host.pem").write_bytes(
        key_pem(wrong_host_key) + cert_pem(wrong_host_cert)
    )
    print("    kms-wrong-host.pem written (SAN: wronghost.example.com)")


def gen_kms_expired(ca_key: rsa.RSAPrivateKey) -> None:
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


def gen_trusted_ca() -> None:
    # Separate CA unrelated to the main Drivers Testing CA; used in CA-bundle tests only.
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
        .add_extension(CA_KEY_USAGE, critical=True)
        .sign(trusted_ca_key, hashes.SHA256())
    )
    (SCRIPT_DIR / "trusted-ca.pem").write_bytes(cert_pem(trusted_ca_cert))
    print("    trusted-ca.pem written")


def verify() -> int:
    print()
    print("==> Verifying cert properties...")
    errors = 0

    # CA cert must have critical basicConstraints, critical keyUsage, and SKI; must NOT have AKI or SAN.
    ca_text = cert_text(SCRIPT_DIR / "ca.pem")
    prev_errors = errors
    if "Basic Constraints: critical" not in ca_text:
        print(
            "    ca.pem: ERROR — basicConstraints not critical (required by Python 3.14 strict mode)",
            file=sys.stderr,
        )
        errors += 1
    if "Key Usage: critical" not in ca_text:
        print(
            "    ca.pem: ERROR — missing critical keyUsage (required by Python 3.14 strict mode)",
            file=sys.stderr,
        )
        errors += 1
    if "Subject Key Identifier" not in ca_text:
        print(
            "    ca.pem: ERROR — missing SKI (required for keyid-form AKI on KMS leaf certs)",
            file=sys.stderr,
        )
        errors += 1
    for ext in ("Authority Key Identifier", "Subject Alternative Name"):
        if ext in ca_text:
            print(
                f"    ca.pem: ERROR — has {ext} (would cause issues on Windows or macOS)",
                file=sys.stderr,
            )
            errors += 1
    if errors == prev_errors:
        print("    ca.pem: OK (has basicConstraints critical, keyUsage critical, SKI; no AKI/SAN)")

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
        prev_errors = errors
        if "Authority Key Identifier" not in text:
            print(f"    {name}: ERROR — missing AKI (required for Python 3.13+)", file=sys.stderr)
            errors += 1
        elif "keyid:" not in text.lower() and "Key Identifier" not in text:
            print(
                f"    {name}: ERROR — AKI missing keyIdentifier (OpenSSL 3.3+ strict mode requires keyid form)",
                file=sys.stderr,
            )
            errors += 1
        if "Subject Key Identifier" not in text:
            print(f"    {name}: ERROR — missing SKI (required for Python 3.14+)", file=sys.stderr)
            errors += 1
        if errors == prev_errors:
            print(f"    {name}: OK (has keyid-form AKI + SKI)")

    return errors


def main() -> None:
    ca_key = gen_ca()
    gen_server(ca_key)
    gen_kms_server(ca_key)
    gen_client(ca_key)
    gen_crl(ca_key)
    gen_kms_wrong_host(ca_key)
    gen_kms_expired(ca_key)
    gen_trusted_ca()

    if verify():
        sys.exit(1)

    print()
    print("Done. All certificates regenerated.")


if __name__ == "__main__":
    main()
