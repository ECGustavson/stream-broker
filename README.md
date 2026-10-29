# Stream Broker

TCP bridge between a Rice Lake 680 indicator and Ignition SCADA via OPC UA. The 680 sends weight continuously in the standard Rice Lake stream format. The broker parses each frame and writes live weight, mode and status to OPC UA tags. Ignition sends zero, tare and gross/net commands back through a writable `Command` tag, and the broker forwards them to the indicator on the same socket.

Built from the tcp-broker (1280) template. The 1280 CSV record and its ACK handshake are gone; the stream needs no acknowledgement.

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

Edit `scales.json` with your scale names, IPs, ports, and connection mode, then run:

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
      "name": "Scale680_01",
      "mode": "listen",
      "ip": "10.10.10.11",
      "port": 10010,
      "enabled": true,
      "description": "Optional label",
      "units": { "P": "LB", "S": "KG" },
      "stream_timeout_seconds": 5
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

**`mode`**: who opens the TCP connection. Default `"listen"`.
- `"listen"`: the 680 is a TCP client and connects to the broker on `port`. `ip` is a whitelist; set it to `null` or omit it to accept any source. Only one connection per scale is accepted at a time.
- `"connect"`: the 680 is a TCP server and the broker connects to `ip`:`port`, reconnecting every `reconnect_seconds` (default 5) if the link drops.

**`units`**: optional. Maps the stream's unit code (`P`/`S`/`T`) to the label written to `WeightUnits`. Unmapped codes are published as the raw letter.

**`stream_timeout_seconds`**: if no data arrives for this long, the connection is dropped and `StreamActive`/`Connected` go false. Default 5. Must be longer than the indicator's stream interval.

**`allow_no_security`**: set `true` to run without OPC UA certificates. If certs are needed, run `gen_cert.py` first and set this to `false`.

---

## OPC UA tags

Each scale gets its own folder named from the `name` field in config. Tags within each folder:

| Tag | Type | Notes |
|---|---|---|
| `Weight` | Double | Signed weight. Not updated on out-of-range/invalid frames that carry no number |
| `WeightDisplay` | String | Signed weight as sent, keeps the indicator's decimal places (e.g. `-12.50`) |
| `WeightUnits` | String | Mapped via `units` in config, else raw `P`/`S`/`T` |
| `Mode` | String | `GROSS`, `NET`, or `TARE` |
| `Status` | String | `OK`, `MOTION`, `OUT_OF_RANGE`, `INVALID`, `COZ` |
| `Motion` | Boolean | Status = motion |
| `CenterOfZero` | Boolean | Status = center of zero |
| `Command` | String | **Writable.** Ignition writes a command here; the broker sends it and clears the tag |
| `CommandResponse` | String | Indicator reply (`OK` / `??`), or `REJECTED: ...` from the broker |
| `LastCommand` | String | Last command actually sent to the indicator |
| `Connected` | Boolean | TCP connection up |
| `StreamActive` | Boolean | Valid frames are arriving |
| `FrameCount` | Int64 | Valid frames this session. Updated once per second |
| `SecondsSinceLastFrame` | Int64 | -1 until first frame |
| `LastError` | String | Last parse failure. Clears on next good frame |

Weight tags are only written when the reading changes, so a stable scale doesn't flood OPC with identical writes. After a disconnect the weight tags keep their last value. Check `StreamActive` before trusting them.

---

## Commands

Ignition writes one of these strings to `Command` (case-insensitive):

| Command | Action |
|---|---|
| `KZERO` | Zero |
| `KTARE` | Tare |
| `KGROSSNET` | Toggle gross/net |
| `KGROSS` | Gross mode |
| `KNET` | Net mode |
| `KCLRTAR` | Clear tare (also returns to gross) |

The broker polls `Command` every 100ms, clears it, and sends the command CRLF-terminated. It clears `CommandResponse` first, so the indicator's reply always fires a tag change event, even if it's the same `OK` as last time. Any line from the indicator that isn't a stream frame (no STX) is treated as the reply.

The broker rejects anything that isn't in the list above with `REJECTED: unknown command`, so a bad tag write can't push configuration commands to the indicator. It also rejects commands written while the indicator is disconnected (`REJECTED: indicator not connected`) instead of queuing them. Otherwise a stale `KZERO` could run when the scale reconnects.

---

## Console

The `>` prompt appears on startup.

| Command | Description |
|---|---|
| `status` | All scales: connected, stream active, current reading |
| `status <name>` | Detail including last command and response |
| `list` | All scales from config |
| `enable <name>` | Enable in config |
| `disable <name>` | Disable in config |
| `rename <old> <new>` | Rename in config |
| `reload` | Reload config + restart TCP listeners/connections |
| `loglevel <name> <LEVEL>` | DEBUG / INFO / WARNING / ERROR. DEBUG logs every raw frame |
| `tail <name> [n]` | Last n log lines (default 20) |
| `send <name> <COMMAND>` | Send a command to the indicator, for testing without Ignition |
| `certstatus` | Cert expiry, SANs, trusted store contents |
| `quit` | Shut down |

`enable`, `disable`, `rename` write to `scales.json` immediately but require `reload` to take effect. Renaming a scale requires a full broker restart to update the OPC UA folder name.

---

## Stream format

```
<STX><P><W7.><U><M><S><CR><LF>
```

| Field | Width | Values |
|---|---|---|
| STX | 1 | ASCII 0x02 |
| P | 1 | Polarity: space = +, `-` = - |
| W7. | 7 | Weight incl. decimal point, leading spaces |
| U | 1 | Units: `P` primary, `S` secondary, `T` tertiary |
| M | 1 | Mode: `G` gross, `N` net, `T` tare |
| S | 1 | Status: space OK, `M` motion, `O` out of range, `I` invalid, `Z` center of zero |

Example: `<STX>  1234.5PG <CR><LF>` is +1234.5, primary units, gross, OK.

---

## Ignition connection

`Config → OPC UA → Connections → Add Connection`

Point it at `opc.tcp://<broker-ip>:<port>/broker`. For dual-broker redundancy, add a second connection as the failover endpoint. Ignition switches automatically if the primary goes unreachable. A 680 with only one socket can only stream to one broker at a time, so redundancy depends on the indicator's ethernet config.

Bind the weight/status tags directly. For buttons, write the command string to the `Command` tag and watch `CommandResponse` for `OK` / `??`.

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

**Scale not connecting (listen mode)**: check the broker log for a "Rejected connection" line. Either the source IP doesn't match the `ip` field, or a connection for that scale is already open. Update the config, or set `ip` to `null` to accept any source.

**Scale not connecting (connect mode)**: the log shows "Connect to ... failed" once, then keeps retrying quietly. Check the indicator's IP/port and that its ethernet port is set up as a server.

**Parse errors in LastError**: set `loglevel <name> DEBUG` to see raw frames. The frame after STX must be exactly 11 characters. If the indicator's stream format was customized, or the weight field is wider or narrower than 7 characters, it won't match.

**Connection keeps dropping**: `stream_timeout_seconds` is shorter than the gap between frames, or streaming is turned off on the indicator.

**Command returns `??`**: the indicator received the command but rejected it (e.g. tare in motion, or the command isn't valid in the current mode).
