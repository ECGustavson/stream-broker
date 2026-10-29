# Scale Broker

TCP bridge between Rice Lake 1280 indicators and Ignition SCADA via OPC UA. The 1280 connects outbound as a TCP client; the broker listens, parses the CSV record, writes to OPC UA tags, and waits for Ignition to send an ACK back through a writable tag. 

Supports two 1280 user programs — finish scale (manual print key) and rail scale (automatic peak detection). Designed for dual-instance redundancy where both brokers run simultaneously and each 1280 connects to both.

Most 920i's on site are configured as TCP servers. We may want to change this going forward, or can continue using the stock Ignition driver for 920i's, but the hardwired ethernet cards only support one port connection, meaning redundancy can't be used on the 920i's without additional hardware or software. 

---

## Requirements

- Python 3.12+
- `asyncua`
- `cryptography` (only needed if using OPC UA security, so not needed for current configuration - only referenced in gen_cert.py)

```bash
pip install asyncua cryptography
```

---

## Setup

Edit `scales.json` with your scale names, IPs, ports, and program types, then run:

```bash
python broker.py
```

Run inside `tmux` if you want the console to survive SSH disconnects:

```bash
tmux new -s broker
python broker.py
# Ctrl-B D to detach
# tmux attach -t broker to come back
```

---

## scales.json

```json
{
  "scales": [
    {
      "name": "FinishScale_01",
      "program": "finish",
      "port": 10010,
      "ip": "192.168.1.101",
      "enabled": true,
      "description": "Optional label",
      "ack_timeout_seconds": 5
    }
  ],
  "opc": {
    "endpoint": "opc.tcp://0.0.0.0:4842/broker",
    "namespace": "http://floweigh/scalebroker",
    "pki_dir": "pki",
    "allow_no_security": true
  },
  "logging": {
    "log_dir": "logs",
    "max_bytes": 5242880,
    "backup_count": 10
  }
}
```

**`program`** — `"finish"` or `"rail"`, case-insensitive.

**`ip`** — set to `null` or omit to accept connections from any IP on that port. Useful if multiple 1280s share one port.

**`ack_timeout_seconds`** — how long the broker holds the connection open waiting for Ignition to write the ACK. Default 5.

**`allow_no_security`** — set `true` to run without OPC UA certificates. If certs are needed, run `gen_cert.py` first and set this to `false`.

---

## OPC UA tags

Each scale gets its own folder named from the `name` field in config. Tags within each folder:

| Tag | Type | Notes |
|---|---|---|
| `GrossWeight` | String | |
| `TareWeight` | String | |
| `NetWeight` | String | |
| `H1Gross` | String | Rail only — first half peak gross |
| `H1Net` | String | Rail only — first half peak net |
| `H2Gross` | String | Rail only — second half. Empty if skip side pressed or whole sow |
| `H2Net` | String | Rail only — second half. Empty if skip side pressed or whole sow |
| `Serial` | String | |
| `ScaleID` | String | Rail: 4=dynamic, 9=static. Finish: supervisor menu configured|
| `ScaleName` | String | Rail: always RAIL. Finish: supervisor menu configured |
| `KillID` | String | Rail: sequential head count, resets midnight. Finish: always ''. |
| `LotCode` | String | |
| `WeightUnits` | String | e.g. LB, KG |
| `Temperature` | String | Finish only |
| `TempUnits` | String | Finish only |
| `PrinterNumber` | String | Finish only |
| `OrderNumber` | String | Finish only |
| `Timestamp` | String | YYYYMMDDHHMMSS |
| `TransactionID` | String | 18-char deduplication key |
| `Message` | String | Full raw CSV — Ignition watches this tag for new records |
| `Writable` | String | Ignition writes the ACK string here |
| `HandshakeAgain` | Boolean | Write True to resend the last ACK without re-inserting to DB |
| `RawRecord` | String | Same as Message — kept for diagnostics |
| `Connected` | Boolean | |
| `RecordCount` | Int32 | Resets on broker restart |
| `SecondsSinceLastRecord` | Int32 | -1 until first record received |
| `LastError` | String | Last parse failure. Clears on next good record. |

---

## ACK handshake

After writing the OPC tags, the broker holds the TCP connection open and polls `Writable` every 100ms for up to `ack_timeout_seconds`. Ignition's tag change script fires on `Message`, inserts to the database, then writes the ACK string to `Writable`. Operation to be confirmed through testing. 

ACK format: `F#1=OK{TransactionID}{HHmmDDMMyy}` + CRLF

The `F#1=` prefix triggers the 1280's `Cmd1Handler`. The remaining 30 characters (`OK` + 18-char TransactionID + 10-char timestamp) are what the 1280 validates. If the length check passes, the 1280 also syncs its RTC to the returned timestamp.

`F#2=OKTEST` can be used on the rail scale to simulate limit switch trips

**HandshakeAgain** — if the 1280 didn't receive the ACK and reconnects to retry, write `True` to `HandshakeAgain`. The broker will resend the cached ACK from the previous transaction without Ignition needing to re-run the DB insert. Currenlty unsure of use case, since scale closes connection after a programmable time in the config menu. This may be from 920i server ports, where Ignition always held the connection open unless manually disabled. 

---

## Console

The `>` prompt appears on startup.

| Command | Description |
|---|---|
| `status` | All scales — connected, record count, last record age |
| `status <name>` | Detail including last TransactionID and last ACK sent |
| `list` | All scales from config |
| `enable <name>` | Enable in config |
| `disable <name>` | Disable in config |
| `rename <old> <new>` | Rename in config |
| `reload` | Reload config + restart TCP listeners |
| `loglevel <name> <LEVEL>` | DEBUG / INFO / WARNING / ERROR |
| `tail <name> [n]` | Last n log lines (default 20) |
| `ack <name>` | Manually ACK last record on a scale — for testing without Ignition |
| `ack last` | Same, but targets whichever scale most recently received a record |
| `certstatus` | Cert expiry, SANs, trusted store contents |
| `quit` | Shut down |

`enable`, `disable`, `rename` write to `scales.json` immediately but require `reload` to take effect. Renaming a scale requires a full broker restart to update the OPC UA folder name.

---

## Message formats

Both programs use the same 20-field comma-delimited layout, CRLF terminated.

**Finish** (fields 8–11 always empty, fields 13–16 populated):
```
Serial,,ScaleName,0,Lot,Gross,Tare,Net,,,,,Units,Temp,TempUnits,Printer,Order,YYYYMMDD,HHMMSS,TransactionID
```

**Rail** (fields 8–11 populated, fields 13–16 always empty — note four consecutive commas):
```
Serial,ScaleID,RAIL,KillID,Lot,Gross,Tare,Net,H1Gross,H1Net,H2Gross,H2Net,Units,0.0,,,,YYYYMMDD,HHMMSS,TransactionID
```

---

## Ignition connection

`Config → OPC UA → Connections → Add Connection`

Point it at `opc.tcp://<broker-ip>:<port>/broker`. For dual-broker redundancy, add a second connection as the failover endpoint — Ignition will switch automatically if the primary goes unreachable.

The Ignition UDT watches `Message` for new records and writes ACKs to `Writable`. Finish scales use a tag called `Writable` on the Ignition side; rail scales use `Handshake` — both map to the broker's `Writable` tag.

---

## Balena Deployment

The broker is designed to run as a Balena fleet app on a Raspberry Pi 4 (4GB RAM). Ignition and MariaDB should run on a separate x86 machine — the Pi does not have enough RAM to run Ignition alongside the broker.

### docker-compose.yml

```yaml
version: "2.4"

services:
  broker:
    build: .
    restart: always
    network_mode: host
    volumes:
      - broker-config:/data/config
      - broker-logs:/data/logs

volumes:
  broker-config:
  broker-logs:
```

`restart: always` is required — Balena does not support `unless-stopped`.

`network_mode: host` puts the broker directly on the host network, which is required for OPC UA and TCP listeners to be reachable without explicit port mapping.

### Console in a container

The interactive console (`>` prompt) is disabled when there is no TTY. To enable it in a Balena/Docker deployment, add to the service in `docker-compose.yml`:

```yaml
tty: true
stdin_open: true
```

Without these, the broker runs in background mode and all interaction is through the Balena logs dashboard.

### Test config (WiFi)

Tested on a Raspberry Pi 4 (4GB RAM) running Balena OS, connected via WiFi. Fleet name: `scale-broker`. OPC UA reachable at `opc.tcp://<device-ip>:4842/broker`.

### Known issues

**`system-connections/ethernet.nmconnection` does not reliably apply via `balena push`.**
The repo contains `system-connections/ethernet.nmconnection` configured for a static IP of `10.10.10.10/24` on `eth0`. Despite the file being present and not excluded by `.dockerignore` or `.gitignore`, it does not consistently appear in `/mnt/boot/system-connections/` on the device after a push and reboot.

Workaround — write the file directly on the device over SSH:

```bash
cat > /mnt/boot/system-connections/ethernet.nmconnection << 'EOF'
[connection]
id=ethernet
type=ethernet
interface-name=eth0

[ethernet]

[ipv4]
method=manual
address1=10.10.10.10/24

[ipv6]
method=auto
EOF
chmod 600 /mnt/boot/system-connections/ethernet.nmconnection
nmcli connection reload
nmcli connection up ethernet
```

---

## Troubleshooting

**Scale not connecting** — check the broker log for a "Rejected connection" line. The source IP doesn't match the `ip` field. Either update the config or set `ip` to `null` to accept any source, functionally disabling the whitelist

**Parse errors in LastError** — check `RawRecord` for the raw string. Field count must be exactly 20 to pass parser check. Rail scale messages have four consecutive commas between Units and Date (`LB,0.0,,,,20260421`).

**ACK timeout warnings** — Ignition didn't write to `Writable` within the timeout window. Check the Ignition tag change script is enabled and the DB connection is healthy. Use `ack <name>` in the console to test the TCP path independently. Default time values are hardcoded, but can be updated before deployment after testing, or always include time value in scales.json

**OPC tags show Bad quality** — OPC UA connection is down. If running with `allow_no_security: false`, check `certstatus` and verify the cert exchange with Ignition is complete.

**1280 keeps retrying** — the rail scale retransmits every ~60 seconds until ACKed. Check `status <name>` for the last ACK sent. If needed, use `HandshakeAgain` or `ack <name>` to manually push an ACK.