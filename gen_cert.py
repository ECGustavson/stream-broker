"""
gen_cert.py — Generate a self-signed OPC UA certificate for the Scale Broker.
Run once on each broker host before starting broker.py.

Usage:
    python gen_cert.py --host 192.168.1.10 --name "ScaleBroker_PlantA"

Arguments:
    --host   IP address or hostname Ignition will connect to (goes into SAN)
    --name   Certificate common name — use something that identifies the host
    --days   Certificate validity in days (default: 3650 = ~10 years)
    --out    PKI directory to write into (default: pki/)
"""

import argparse
import ipaddress
from datetime import datetime, timezone, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def gen_cert(host: str, name: str, days: int, pki_dir: Path):
    pki_dir.mkdir(parents=True, exist_ok=True)
    (pki_dir / "trusted").mkdir(exist_ok=True)
    (pki_dir / "rejected").mkdir(exist_ok=True)

    # ── Private key ────────────────────────────────────────────────────────────
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # ── Subject Alternative Name ───────────────────────────────────────────────
    # OPC UA requires the ApplicationUri in the SAN URI field.
    # Also add the host as IP or DNS so TLS validation passes.
    app_uri = f"urn:{host}:floweigh:scalebroker"
    try:
        san_host = x509.IPAddress(ipaddress.ip_address(host))
    except ValueError:
        san_host = x509.DNSName(host)

    # ── Certificate ────────────────────────────────────────────────────────────
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME,       name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Floweigh LLC"),
        x509.NameAttribute(NameOID.COUNTRY_NAME,      "US"),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)   # self-signed: issuer == subject
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=days))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.UniformResourceIdentifier(app_uri),
                san_host,
            ]),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=True,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.SERVER_AUTH,
                ExtendedKeyUsageOID.CLIENT_AUTH,
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    # ── Write files ────────────────────────────────────────────────────────────
    cert_path = pki_dir / "broker_cert.pem"
    key_path  = pki_dir / "broker_key.pem"

    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ))
    key_path.chmod(0o600)   # owner read/write only

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    print("Certificate generated successfully")
    print("─" * 50)
    print(f"  Cert:            {cert_path}")
    print(f"  Key:             {key_path}  (chmod 600)")
    print(f"  Common name:     {name}")
    print(f"  Application URI: {app_uri}")
    print(f"  Valid until:     {cert.not_valid_after_utc.strftime('%Y-%m-%d')} ({days} days)")
    print()
    print("Next steps")
    print("─" * 50)
    print(f"  1. Import broker cert into Ignition:")
    print(f"       Ignition > Config > OPC UA > Security > Trusted Certificates > Import")
    print(f"       File: {cert_path.resolve()}")
    print()
    print(f"  2. Export Ignition's certificate:")
    print(f"       Ignition > Config > OPC UA > Security > Server Certificate > Export (DER format)")
    print(f"       Save to: {(pki_dir / 'trusted' / 'ignition_cert.der').resolve()}")
    print()
    print(f"  3. Start the broker:")
    print(f"       python broker.py")
    print()
    print(f"  4. Verify with console command: certstatus")
    print()
    print("Dual-broker note")
    print("─" * 50)
    print("  Run gen_cert.py separately on each broker host with its own")
    print("  --host IP. Ignition must trust BOTH certs (one per OPC UA connection).")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate OPC UA certificate for Scale Broker")
    parser.add_argument("--host", required=True, help="Broker host IP or hostname (goes into certificate SAN)")
    parser.add_argument("--name", default="ScaleBroker",  help="Certificate common name")
    parser.add_argument("--days", default=3650, type=int, help="Validity period in days (default: 3650)")
    parser.add_argument("--out",  default="pki",          help="PKI output directory (default: pki/)")
    args = parser.parse_args()

    gen_cert(
        host    = args.host,
        name    = args.name,
        days    = args.days,
        pki_dir = Path(args.out),
    )
