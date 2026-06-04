#!/usr/bin/env bash
# Regenerate all TLS test certificates with proper Authority Key Identifier (AKI)
# and Subject Key Identifier (SKI) extensions.
#
# Usage: bash gen-certs.sh  (run from test/certificates/)
#
# Prerequisites: OpenSSL 1.1+ or LibreSSL 3+
# Password for password_protected.pem: qwerty
# See README.md for full details.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

DAYS=7300  # ~20 years

# ----------------------------------------------------------------------------
# OpenSSL extension config
# ----------------------------------------------------------------------------
cat > "$TMPDIR/ext.cnf" << 'EOF'
[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:TRUE

[ v3_server ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1

[ v3_client ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF

# ----------------------------------------------------------------------------
# OpenSSL CA config (for CRL generation)
# ----------------------------------------------------------------------------
mkdir -p "$TMPDIR/cadb/newcerts"
touch "$TMPDIR/cadb/index.txt"
printf '01\n' > "$TMPDIR/cadb/serial"
printf '01\n' > "$TMPDIR/cadb/crlnumber"

cat > "$TMPDIR/ca.cnf" << EOF
[ ca ]
default_ca = CA_default

[ CA_default ]
dir               = $TMPDIR/cadb
new_certs_dir     = $TMPDIR/cadb/newcerts
database          = $TMPDIR/cadb/index.txt
serial            = $TMPDIR/cadb/serial
crlnumber         = $TMPDIR/cadb/crlnumber
certificate       = $TMPDIR/ca.pem
private_key       = $TMPDIR/ca.key
default_days      = $DAYS
default_crl_days  = $DAYS
default_md        = sha256
preserve          = no
policy            = policy_match

[ policy_match ]
countryName            = optional
stateOrProvinceName    = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional
EOF

# ----------------------------------------------------------------------------
# 1. Drivers Testing CA
# ----------------------------------------------------------------------------
echo "==> Generating Drivers Testing CA..."
openssl genrsa -out "$TMPDIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days $DAYS \
    -key "$TMPDIR/ca.key" \
    -out "$TMPDIR/ca.pem" \
    -subj "/C=US/ST=New York/L=New York City/O=MongoDB/OU=Drivers/CN=Drivers Testing CA" \
    -extensions v3_ca \
    -config "$TMPDIR/ext.cnf"

cp "$TMPDIR/ca.pem" "$SCRIPT_DIR/ca.pem"
echo "    ca.pem written"

# ----------------------------------------------------------------------------
# 2. Server certificate
# ----------------------------------------------------------------------------
echo "==> Generating server certificate..."
openssl genrsa -out "$TMPDIR/server.key" 2048 2>/dev/null
openssl req -new \
    -key "$TMPDIR/server.key" \
    -out "$TMPDIR/server.csr" \
    -subj "/C=US/ST=New York/L=New York City/O=MongoDB/OU=Drivers/CN=localhost"
openssl x509 -req -days $DAYS \
    -in "$TMPDIR/server.csr" \
    -CA "$TMPDIR/ca.pem" \
    -CAkey "$TMPDIR/ca.key" \
    -CAcreateserial \
    -out "$TMPDIR/server.crt" \
    -extfile "$TMPDIR/ext.cnf" \
    -extensions v3_server 2>/dev/null

# server.pem = private key + certificate
cat "$TMPDIR/server.key" "$TMPDIR/server.crt" > "$SCRIPT_DIR/server.pem"
echo "    server.pem written"

# ----------------------------------------------------------------------------
# 3. Client certificate
# ----------------------------------------------------------------------------
echo "==> Generating client certificate..."
openssl genrsa -out "$TMPDIR/client.key" 2048 2>/dev/null
openssl req -new \
    -key "$TMPDIR/client.key" \
    -out "$TMPDIR/client.csr" \
    -subj "/C=US/ST=New York/L=New York City/O=MDB/OU=Drivers/CN=client"
openssl x509 -req -days $DAYS \
    -in "$TMPDIR/client.csr" \
    -CA "$TMPDIR/ca.pem" \
    -CAkey "$TMPDIR/ca.key" \
    -CAserial "$TMPDIR/ca.srl" \
    -out "$TMPDIR/client.crt" \
    -extfile "$TMPDIR/ext.cnf" \
    -extensions v3_client 2>/dev/null

# client.pem = private key + certificate
cat "$TMPDIR/client.key" "$TMPDIR/client.crt" > "$SCRIPT_DIR/client.pem"
echo "    client.pem written"

# ----------------------------------------------------------------------------
# 4. Password-protected client certificate
# ----------------------------------------------------------------------------
echo "==> Generating password-protected client certificate..."
openssl rsa -in "$TMPDIR/client.key" \
    -aes256 -passout pass:qwerty \
    -out "$TMPDIR/client_enc.key" 2>/dev/null

# password_protected.pem = encrypted key + certificate (same cert as client)
cat "$TMPDIR/client_enc.key" "$TMPDIR/client.crt" > "$SCRIPT_DIR/password_protected.pem"
echo "    password_protected.pem written (password: qwerty)"

# ----------------------------------------------------------------------------
# 5. CRL (empty — no revoked certs)
# ----------------------------------------------------------------------------
echo "==> Generating CRL..."
openssl ca -config "$TMPDIR/ca.cnf" -gencrl -out "$SCRIPT_DIR/crl.pem" 2>/dev/null
echo "    crl.pem written"

# ----------------------------------------------------------------------------
# 6. Trusted Kernel Test CA (trusted-ca.pem)
#    A separate CA used in CA-bundle tests; does NOT sign server/client certs.
# ----------------------------------------------------------------------------
echo "==> Generating Trusted Kernel Test CA..."
cat > "$TMPDIR/trusted_ext.cnf" << 'EOF'
[ v3_trusted_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:TRUE
EOF

openssl genrsa -out "$TMPDIR/trusted_ca.key" 2048 2>/dev/null
openssl req -new -x509 -days $DAYS \
    -key "$TMPDIR/trusted_ca.key" \
    -out "$SCRIPT_DIR/trusted-ca.pem" \
    -subj "/C=US/ST=New York/L=New York City/O=MongoDB/OU=Kernel/CN=Trusted Kernel Test CA" \
    -extensions v3_trusted_ca \
    -config "$TMPDIR/trusted_ext.cnf"
echo "    trusted-ca.pem written"

# ----------------------------------------------------------------------------
# Verify
# ----------------------------------------------------------------------------
echo ""
echo "==> Verifying AKI is present..."
for cert in ca.pem server.pem client.pem trusted-ca.pem; do
    result=$(openssl x509 -noout -text -in "$SCRIPT_DIR/$cert" 2>/dev/null | grep "Authority Key Identifier" | head -1)
    if [ -n "$result" ]; then
        echo "    $cert: OK ($result)"
    else
        echo "    $cert: MISSING AKI - check generation!" >&2
        exit 1
    fi
done

echo ""
echo "Done. All certificates regenerated with AKI."
