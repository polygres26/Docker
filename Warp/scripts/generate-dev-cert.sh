#!/usr/bin/env bash
# Generates a self-signed TLS keystore for local warp development/testing.
# Not for production use - production deployments should provision a
# keystore signed by a real CA instead of running this script.
set -euo pipefail

OUT_DIR="${1:-$(dirname "$0")/../certs}"
ALIAS="warp"
STOREPASS="${WARP_TLS_KEYSTORE_PASSWORD:-changeit}"
CN="${WARP_TLS_CERT_CN:-localhost}"

mkdir -p "$OUT_DIR"
KEYSTORE="$OUT_DIR/warp_dev.p12"
CERT_PEM="$OUT_DIR/warp_dev_cert.pem"

rm -f "$KEYSTORE" "$CERT_PEM"

keytool -genkeypair \
    -alias "$ALIAS" \
    -keyalg RSA -keysize 2048 -validity 3650 \
    -keystore "$KEYSTORE" -storetype PKCS12 \
    -storepass "$STOREPASS" -keypass "$STOREPASS" \
    -dname "CN=$CN, OU=warp dev, O=warp, L=Dev, ST=Dev, C=US"

keytool -exportcert \
    -alias "$ALIAS" \
    -keystore "$KEYSTORE" -storetype PKCS12 -storepass "$STOREPASS" \
    -rfc -file "$CERT_PEM"

cat <<EOF

Generated:
  server keystore: $KEYSTORE
  client trust cert (PEM): $CERT_PEM

Run the server with TLS enabled:
  WARP_TLS_KEYSTORE=$KEYSTORE WARP_TLS_KEYSTORE_PASSWORD=$STOREPASS ./scripts/run.sh

Connect a python-oracledb client over TCPS, trusting this dev cert:
  import ssl, oracledb
  ctx = ssl.create_default_context(cafile="$CERT_PEM")
  oracledb.connect(user=..., password=..., dsn="localhost:2484/orcl",
                    protocol="tcps", ssl_context=ctx, ssl_server_dn_match=False)
EOF
