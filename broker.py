"""
Scale Broker — Rice Lake 1280 TCP -> OPC UA Bridge
---------------------------------------------------
Supports two Rice Lake 1280 user programs, identified in scales.json
by the "program" field (case-insensitive):

  "finish"  — finish scale, manual print key
  "rail"    — rail scale, automatic peak detection

Designed for multi-instance in Docker containers, with second instance as fallback.
User software on all scales will need to be updated to utilize TCPC2 for the fallback instance.
Config-driven scale naming, per-scale logging, watchdog tags,
OPC UA Sign & Encrypt (Basic256Sha256), and SSH management console.

Usage:
    python broker.py [--config scales.json]

Console commands (type 'help' at the > prompt):
    status                       Show all scale status summary
    status <name>                Show detail for one scale
    list                         List configured scales
    enable  <name>               Enable a scale (save + reload to apply)
    disable <name>               Disable a scale (save + reload to apply)
    rename  <old> <new>          Rename a scale (save + reload to apply)
    reload                       Reload config and restart TCP listeners
    loglevel <name> <LEVEL>      Set log level DEBUG/INFO/WARNING/ERROR
    tail <name> [lines]          Print last n lines of scale log (default 20)
    ack <name>                   Manually ACK last record on a scale (test without Ignition)
    ack last                     Manually ACK last record on whichever scale most recently received one
    certstatus                   Show certificate and security status
    quit                         Shut down broker
    help                         Show this help

"finish" message format (20 fields, CRLF terminated):
  0:ScaleNo, 1:ScaleID, 2:ScaleType(PIT, BENCH, etc), 3:KillID(unused), 4:Lot/Sequence(not currently used),
  5:Gross, 6:Tare, 7:Net, 8:H1Gross(unused), 9:H1Net(unused),
  10:H2Gross(unused), 11:H2Net(unused), 12:Units, 13:Temp, 14:TempUnits,
  15:Printer, 16:Order, 17:Date(YYYYMMDD), 18:Time(HHMMSS),
  19:TransactionID(18 chars: MMDDYY+HHMMSS+000000)

"rail" message format (20 fields, CRLF terminated) - New String:
  0:ScaleNo, 1:ScaleID, 2:ScaleType(RAIL), 3:KillID, 4:Lot/Sequence,
  5:Gross, 6:Tare, 7:Net, 8:H1Gross, 9:H1Net,
  10:H2Gross, 11:H2Net, 12:Units, 13:Temp(0.0), 14:TempUnits(empty),
  15:Printer(empty), 16:Order(empty), 17:Date(YYYYMMDD), 18:Time(HHMMSS),
  19:TransactionID(18 chars: MMDDYY+HHMMSS+Right("000000"+KillID,6))

ACK handshake:
  Format: F#1=OK{TransactionID}{HHmmDDMMyy}  (30 chars + CRLF)

  Normal flow:
    1. 1280 sends record over TCP
    2. Broker writes Message tag (clears it first to force change event in Ignition)
    3. Ignition's tag change script on Message fires — does DB insert,
       builds ACK string, writes it to the Writable tag
    4. Broker polls Writable, picks up the ACK string, sends to 1280

  Reconnect / retry flow (handled entirely on the Ignition side):
    1. 1280 didn't receive previous ACK and reconnects, retransmitting the record
    2. Operator (or Ignition logic) sets HandshakeAgain = True
    3. Ignition's tag change script on HandshakeAgain fires — same handler
       as Message change, but should dedupe on TransactionID before re-inserting
    4. Ignition writes ACK string to Writable
    5. Broker picks it up and sends to 1280 as normal

  The broker waits indefinitely for Writable to be populated. The only
  escape is the 1280 closing the TCP socket, which causes readline() to
  return and the handler to exit cleanly. The 1280 will then reconnect
  on its retry interval.

  Use 'ack <name>' or 'ack last' in the console to test without Ignition.
"""

import asyncio
import argparse
import json
import logging
import time
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from asyncua import Server, ua


# ── Globals ────────────────────────────────────────────────────────────────────

CONFIG_PATH = "scales.json"

_runtime: dict     = {}   # { name: { connected, record_count, last_record_time, last_error,
                            #           last_trans_id, last_ack, config } }
_opc_nodes: dict   = {}   # { name: { tag_name: Node } }
_tcp_servers: list = []
_opc_server: Server | None = None
_make_logger   = None
_broker_log: logging.Logger | None = None
_loggers: dict = {}
_config: dict  = {}


# Config file loading and saving (if modified with console commands) — scales.json is the default but path can be overridden with --config

def load_config(path=CONFIG_PATH) -> dict:
    """
    Load configuration from SCALES_CONFIG env var (preferred) or local file.
    Called before the logger is initialized, so uses print() for diagnostics.
    """
    env_config = os.getenv("SCALES_CONFIG")
    if env_config:
        try:
            print("INFO: Loading configuration from SCALES_CONFIG environment variable.")
            return json.loads(env_config)
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse SCALES_CONFIG JSON: {e}")
            sys.exit(1)

    if os.path.exists(path):
        print(f"INFO: Loading configuration from {path}")
        with open(path) as f:
            return json.load(f)

    print("CRITICAL: No configuration found via SCALES_CONFIG or local file.")
    sys.exit(1)


def save_config(cfg: dict, path=CONFIG_PATH):
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)


# Logging setup - per scale loggers with rotation, plus console output

def setup_logging(log_cfg: dict):
    log_dir      = log_cfg.get("log_dir",      "logs")
    max_bytes    = log_cfg.get("max_bytes",    5_242_880)
    backup_count = log_cfg.get("backup_count", 10)
    Path(log_dir).mkdir(exist_ok=True)

    def make_logger(name: str) -> logging.Logger:
        if name in _loggers:
            return _loggers[name]
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = RotatingFileHandler(
            f"{log_dir}/{name}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
        _loggers[name] = logger
        return logger

    return make_logger


# Record Parsing
#
# Both "finish" and "rail" programs share the same 20-field CSV layout.
# Field positions are identical between programs; the difference is which
# fields carry live data vs. empty strings:
#
#   finish: H1/H2 always empty; Temp/TempUnits/Printer/Order populated
#   rail:   H1/H2 populated with peak weights; Temp/Printer/Order empty
#           KillID is sequential (resets midnight); ScaleType always "RAIL"
#           ScaleID = g_iScale (default 4, dynamic) or 9 (static scale)
#
# The "program" field in scales.json selects which parser is used.
# Accepted values (case-insensitive): "finish", "rail"


# Field index constants — identical layout for both programs
F_SCALENO    = 0
F_SCALE_ID   = 1
F_SCALE_NAME = 2
F_KILL_ID    = 3
F_SEQUENCE   = 4
F_GROSS      = 5
F_TARE       = 6
F_NET        = 7
F_H1_GROSS   = 8
F_H1_NET     = 9
F_H2_GROSS   = 10
F_H2_NET     = 11
F_UNITS      = 12
F_TEMP       = 13
F_TEMP_UNITS = 14
F_PRINTER    = 15
F_ORDER      = 16
F_DATE       = 17   # YYYYMMDD
F_TIME       = 18   # HHMMSS
F_TRANS_ID   = 19   # 18 chars

EXPECTED_FIELDS = 20


def parse_record_finish(fields: list[str]) -> dict:
    """
    "finish" program — Finish Scale.
    Triggered by print key press when net weight > threshold.
    H1/H2 fields are always empty (single-load scale).
    Temperature comes from Scale 2 automatically or manual entry.
    Printer and Order are entered by operator or barcode scan.
    """
    return {
        "GrossWeight":   fields[F_GROSS].strip(),
        "TareWeight":    fields[F_TARE].strip(),
        "NetWeight":     fields[F_NET].strip(),
        "H1Gross":       "",
        "H1Net":         "",
        "H2Gross":       "",
        "H2Net":         "",
        "ScaleNo":       fields[F_SCALENO].strip(),
        "ScaleID":       fields[F_SCALE_ID].strip(),
        "ScaleType":     fields[F_SCALE_NAME].strip(),   # HEADMEAT / PIT / BENCHTOP / custom
        "KillID":        fields[F_KILL_ID].strip(),      # always "0"
        "Sequence":      fields[F_SEQUENCE].strip(),
        "WeightUnits":   fields[F_UNITS].strip().upper(),
        "Temperature":   fields[F_TEMP].strip(),
        "TempUnits":     fields[F_TEMP_UNITS].strip(),   # "F" or "C"
        "PrinterNumber": fields[F_PRINTER].strip(),
        "OrderNumber":   fields[F_ORDER].strip(),
        "Timestamp":     fields[F_DATE].strip() + fields[F_TIME].strip(),
        "TransactionID": fields[F_TRANS_ID].strip(),
    }


def parse_record_rail(fields: list[str]) -> dict:
    """
    "rail" program — Rail Scale (New String format).
    Triggered by limit switch / peak detection or static scale print key.
    H1/H2 fields carry actual peak weights for each half of the animal.
    H2 fields are empty when Skip Side was pressed (single-half or whole-sow mode).
    KillID increments per weighment; resets to 0 at midnight.
    ScaleID = 4 (dynamic hot scale) or 9 (static scale) — set in Supervisor menu.
    Temperature, Printer, and Order are always empty.
    """
    return {
        "GrossWeight":   fields[F_GROSS].strip(),
        "TareWeight":    fields[F_TARE].strip(),
        "NetWeight":     fields[F_NET].strip(),
        "H1Gross":       fields[F_H1_GROSS].strip(),
        "H1Net":         fields[F_H1_NET].strip(),
        "H2Gross":       fields[F_H2_GROSS].strip(),
        "H2Net":         fields[F_H2_NET].strip(),
        "ScaleNo":       fields[F_SCALENO].strip(),
        "ScaleID":       fields[F_SCALE_ID].strip(),     # 4=dynamic, 9=static
        "ScaleType":     fields[F_SCALE_NAME].strip(),   # always "RAIL"
        "KillID":        fields[F_KILL_ID].strip(),
        "Sequence":      fields[F_SEQUENCE].strip(),
        "WeightUnits":   fields[F_UNITS].strip().upper(),
        "Temperature":   "",
        "TempUnits":     "",
        "PrinterNumber": "",
        "OrderNumber":   "",
        "Timestamp":     fields[F_DATE].strip() + fields[F_TIME].strip(),
        "TransactionID": fields[F_TRANS_ID].strip(),     # deduplication key
    }


def parse_record(raw: str, program: str) -> dict:
    """
    Parse a raw CSV line from the 1280, dispatching to the correct
    program parser based on the scale's 'program' field in scales.json.

    Args:
        raw:     Raw ASCII line from the 1280 (may include CRLF)
        program: "finish" or "rail" (case-insensitive)

    Returns:
        Dict of field name -> typed value. All keys match OPC UA tag names.

    Raises:
        ValueError: wrong field count, unknown program
    """
    fields = raw.strip().split(",")

    if len(fields) != EXPECTED_FIELDS:
        raise ValueError(
            f"Expected {EXPECTED_FIELDS} fields, got {len(fields)}. "
            f"Raw: {raw.strip()!r}"
        )

    prog = program.lower().strip()
    if prog == "finish":
        return parse_record_finish(fields)
    elif prog == "rail":
        return parse_record_rail(fields)
    else:
        raise ValueError(
            f"Unknown program '{program}'. Must be 'finish' or 'rail' (case-insensitive)."
        )


# ACK string builder - only used for testing using the ack console command

def build_ack(trans_id: str) -> str:
    """
    Build the ACK string for the 1280 Cmd1Handler.

    The 1280 routes on the "F#1=" prefix, which is stripped before Cmd1Handler
    receives it. sRxData (what Cmd1Handler validates) = "OK" + TransID + timestamp.
    Cmd1Handler checks: Left(sRxData,2)="OK" and Len(sRxData)=30.

    sRxData breakdown:
      "OK"           = 2 chars
      TransactionID  = 18 chars
      HHmmDDMMyy     = 10 chars
      Total          = 30 chars

    Full string sent to 1280: "F#1=" + sRxData + CRLF
    """
    now = datetime.now()
    update_time = now.strftime("%H%M%d%m%y")   # HHmmDDMMyy — 10 chars
    s_rx_data = f"OK{trans_id}{update_time}"    # 30 chars — what Cmd1Handler validates
    return f"F#1={s_rx_data}"                  # full string sent over TCP


# OPC UA SETUP

async def setup_opc_server(cfg: dict, scales: list) -> Server:
    opc_cfg  = cfg["opc"]
    pki_dir  = Path(opc_cfg.get("pki_dir", "pki"))
    cert     = pki_dir / "broker_cert.pem"
    key      = pki_dir / "broker_key.pem"

    server = Server()
    await server.init()

    # THE CRITICAL FIX: Bind to 0.0.0.0 so the container 'answers the door' on the Balena bridge
    server.set_endpoint("opc.tcp://0.0.0.0:4842/broker")
    server.set_server_name("Floweigh Scale Broker")

    # SECURITY - Not currently used - set up for possible future
    if cert.exists() and key.exists():
        await server.load_certificate(str(cert))
        await server.load_private_key(str(key))

        trusted_dir  = pki_dir / "trusted"
        rejected_dir = pki_dir / "rejected"
        trusted_dir.mkdir(exist_ok=True)
        rejected_dir.mkdir(exist_ok=True)
        server.set_security_IDs(["Anonymous"])

        no_security = opc_cfg.get("allow_no_security", False)
        policies    = [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt]
        if no_security:
            policies.insert(0, ua.SecurityPolicyType.NoSecurity)
            _broker_log.warning(
                "NoSecurity policy is ENABLED — disable 'allow_no_security' "
                "in config before production deployment."
            )

        server.set_security_policy(policies)
        server.certificate_validator = await _build_validator(trusted_dir, rejected_dir)
        _broker_log.info("OPC UA security: Basic256Sha256 Sign+Encrypt")
        _broker_log.info(f"Certificate: {cert}")
        _broker_log.info(f"Trust store: {trusted_dir}")
    else:
        _broker_log.warning(
            "PKI cert/key not found — running WITHOUT security (NoSecurity only). "
            f"Run gen_cert.py to generate certificates. Expected: {cert}, {key}"
        )
        server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    # Generate folder and tag tree
    idx     = await server.register_namespace(opc_cfg["namespace"])
    objects = server.nodes.objects

    # Tag definitions: (name, default_value)
    # Single source of truth — easier to maintain than the inline dict.
    TAG_DEFS = [
        ("GrossWeight",            ""),
        ("TareWeight",             ""),
        ("NetWeight",              ""),
        ("H1Gross",                ""),
        ("H1Net",                  ""),
        ("H2Gross",                ""),
        ("H2Net",                  ""),
        ("ScaleNo",                ""),
        ("ScaleID",                ""),
        ("ScaleType",              ""),
        ("KillID",                 ""),
        ("Sequence",               ""),
        ("WeightUnits",            ""),
        ("Temperature",            ""),
        ("TempUnits",              ""),
        ("PrinterNumber",          ""),
        ("OrderNumber",            ""),
        ("Timestamp",              ""),
        ("TransactionID",          ""),
        ("Message",                ""),
        ("Writable",               ""),
        ("HandshakeAgain",         False),
        ("RawRecord",              ""),
        ("Connected",              False),
        ("RecordCount",            0),
        ("SecondsSinceLastRecord", -1),
        ("LastError",              ""),
    ]

    CLIENT_WRITABLE = {"Writable", "HandshakeAgain"}

    for scale in scales:
        name = scale["name"]

        # Explicit string NodeId for the folder: ns=<idx>;s=<ScaleName>
        folder = await objects.add_folder(
            ua.NodeId(name, idx, ua.NodeIdType.String),
            name,
        )

        nodes = {}
        for tag_name, default in TAG_DEFS:
            # Explicit string NodeId for each tag: ns=<idx>;s=<ScaleName>.<TagName>
            node = await folder.add_variable(
                ua.NodeId(f"{name}.{tag_name}", idx, ua.NodeIdType.String),
                tag_name,
                default,
            )
            if tag_name in CLIENT_WRITABLE:
                await node.set_writable()
            nodes[tag_name] = node

        _opc_nodes[name] = nodes

    return server


# Helper to build CertificateValidator for OPC UA security — not currently used, but set up for future use if needed.
async def _build_validator(trusted_dir: Path, rejected_dir: Path):
    from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
    opts = (
        CertificateValidatorOptions.TRUSTED_VALIDATION
        | CertificateValidatorOptions.PEER_SERVER
    )
    return CertificateValidator(
        options=opts,
        trusted_peer_certs=trusted_dir,
        rejected_peer_certs=rejected_dir,
    )


# Watchdog task - timer can be modified after testing

async def watchdog_task():
    """Writes SecondsSinceLastRecord for every active scale every 5 s."""
    while True:
        await asyncio.sleep(5)
        for name, state in list(_runtime.items()):
            if name not in _opc_nodes:
                continue
            lrt = state.get("last_record_time")
            val = -1 if lrt is None else int(time.time() - lrt)
            try:
                await _opc_nodes[name]["SecondsSinceLastRecord"].write_value(val)
            except Exception:
                pass


# Ack sender helper - used by TCP handler and console command

async def send_ack(writer: asyncio.StreamWriter, ack_str: str, name: str, log: logging.Logger):
    """Send ACK string to 1280, appending CRLF if not already present."""
    ack_bytes = (ack_str if ack_str.endswith("\r\n") else ack_str + "\r\n").encode("ascii")
    writer.write(ack_bytes)
    await writer.drain()
    # Cache the ACK for diagnostics and the console 'ack' command
    _runtime[name]["last_ack"] = ack_str.strip()
    log.info(f"ACK sent: {ack_str.strip()}")


# TCP Handler - one per scale, created by make_tcp_handler with scale-specific config closure

def make_tcp_handler(scale: dict):
    name         = scale["name"]
    expected_ip  = scale.get("ip")
    program      = scale.get("program", "finish").lower().strip()
    log          = _make_logger(name)

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer      = writer.get_extra_info("peername")
        client_ip = peer[0] if peer else "unknown"

        if expected_ip and client_ip != expected_ip:
            log.warning(f"Rejected connection from {client_ip} (expected {expected_ip})")
            writer.close()
            return

        if name not in _opc_nodes:
            log.error(f"OPC nodes not ready for '{name}' — closing connection, scale will retry")
            writer.close()
            return

        log.info(f"Connected from {client_ip}")
        _runtime[name]["connected"] = True
        await _opc_nodes[name]["Connected"].write_value(True)
        await _opc_nodes[name]["LastError"].write_value("")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break

                raw = line.decode("ascii", errors="replace")
                log.info(f"RX: {raw.strip()}")

                try:
                    data = parse_record(raw, program)

                    for key, val in data.items():
                        if key in _opc_nodes[name]:
                            await _opc_nodes[name][key].write_value(val)

                    # Write RawRecord, then clear Message and rewrite it.
                    # The clear+rewrite forces a tag change event in Ignition even
                    # if this record is byte-identical to the previous one
                    # (e.g. after Ignition restart with a stale Message value).
                    await _opc_nodes[name]["RawRecord"].write_value(raw.strip())
                    await _opc_nodes[name]["Message"].write_value("")
                    await asyncio.sleep(0.05)
                    await _opc_nodes[name]["Message"].write_value(raw.strip())

                    # Cache TransactionID for the console 'ack' command and diagnostics
                    trans_id = data.get("TransactionID", "")
                    _runtime[name]["last_trans_id"] = trans_id

                    count = _runtime[name]["record_count"] + 1
                    _runtime[name]["record_count"]     = count
                    _runtime[name]["last_record_time"] = time.time()
                    _runtime[name]["last_error"]       = ""
                    await _opc_nodes[name]["LastError"].write_value("") 
                    await _opc_nodes[name]["RecordCount"].write_value(count)

                    log.info(
                        f"Record written | TransID={trans_id} | "
                        f"Net={data.get('NetWeight','')} {data.get('WeightUnits','')} | "
                        f"Waiting for ACK..."
                    )

                    # Poll Writable for ACK string from Ignition.
                    # No timeout — Ignition's tag change script (on Message OR
                    # HandshakeAgain) will populate Writable when ready. The
                    # 1280 closing the socket is the escape hatch: writer
                    # closes, loop exits, outer readline() returns empty.
                    ack_str = ""
                    while not writer.is_closing():
                        await asyncio.sleep(0.1)
                        ack_str = await _opc_nodes[name]["Writable"].read_value()
                        if ack_str:
                            break

                    if ack_str:
                        await send_ack(writer, ack_str, name, log)
                        await _opc_nodes[name]["Writable"].write_value("")
                    else:
                        # Only reachable if the writer closed during the poll loop
                        log.warning(
                            f"Connection closed before ACK received. "
                            f"TransID={trans_id} | 1280 will retry on next connection."
                        )

                except Exception as e:
                    err = f"Parse error: {e} | raw: {raw.strip()}"
                    log.error(err)
                    _runtime[name]["last_error"] = err
                    await _opc_nodes[name]["LastError"].write_value(err)

        except asyncio.IncompleteReadError:
            pass

        finally:
            log.info("Disconnected")
            _runtime[name]["connected"] = False
            try:
                await _opc_nodes[name]["Connected"].write_value(False)
            except Exception:
                pass
            writer.close()

    return handle_client


# TCP server management

async def start_tcp_servers(scales: list):
    for srv in _tcp_servers:
        srv.close()
        await srv.wait_closed()
    _tcp_servers.clear()

    for scale in scales:
        handler = make_tcp_handler(scale)
        srv     = await asyncio.start_server(handler, "0.0.0.0", scale["port"])
        _tcp_servers.append(srv)
        _broker_log.info(
            f"  {scale['name']} [{scale.get('program','?')}] "
            f"listening :{scale['port']}  (IP: {scale.get('ip', 'any')})"
        )
        asyncio.ensure_future(srv.serve_forever())


async def do_reload():
    global _config

    _broker_log.info("Reloading config...")
    try:
        new_cfg = load_config()
    except Exception as e:
        _broker_log.error(f"Config reload failed — {e}")
        return False

    _config   = new_cfg
    enabled   = [s for s in _config["scales"] if s.get("enabled", True)]
    new_names = {s["name"] for s in enabled}

    for name in list(_runtime.keys() - new_names):
        _broker_log.info(f"  Removing scale from runtime: {name}")
        _runtime.pop(name, None)

    for scale in enabled:
        name = scale["name"]
        if name not in _runtime:
            _runtime[name] = {
                "connected":        False,
                "record_count":     0,
                "last_record_time": None,
                "last_error":       "",
                "last_trans_id":    "",
                "last_ack":         "",
                "config":           scale,
            }
            if name not in _opc_nodes:
                _broker_log.warning(
                    f"  '{name}' is new — OPC UA folder requires full restart to appear."
                )
        else:
            _runtime[name]["config"] = scale

    await start_tcp_servers(enabled)
    _broker_log.info("Reload complete.")
    return True


# Console Commands

HELP_TEXT = """
Scale Broker Management Console
─────────────────────────────────────────────────────────────────
  status                        All scales status summary
  status <name>                 Detail view for one scale
  list                          All scales from config
  enable  <name>                Enable scale in config
  disable <name>                Disable scale in config
  rename  <old> <name>          Rename scale in config
  reload                        Reload config + restart TCP listeners
  loglevel <name> <LEVEL>       Set log level (DEBUG/INFO/WARNING/ERROR)
  tail <name> [lines]           Last n log lines for a scale (default 20)
  ack <name>                    Manually ACK last record on named scale
  ack last                      Manually ACK last record across all scales
  certstatus                    Show certificate and security status
  quit                          Shut down broker
  help                          Show this help
─────────────────────────────────────────────────────────────────
Note: enable / disable / rename take effect after 'reload'.
      OPC UA folder renames require a full broker restart.
      'ack' writes to Writable tag — use for testing without Ignition.
      Broker waits indefinitely for Ignition to populate Writable;
      1280 disconnect is the only escape from that wait.
"""


def _fmt_age(secs) -> str:
    if secs is None or secs < 0:
        return "never"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s ago"
    return f"{secs // 3600}h {(secs % 3600) // 60}m ago"


def cmd_status(args):
    if not _runtime:
        print("  No active scales.")
        return

    if args:
        name = args[0]
        if name not in _runtime:
            print(f"  Unknown scale: '{name}'")
            return
        st  = _runtime[name]
        cfg = st["config"]
        lrt = st["last_record_time"]
        age = int(time.time() - lrt) if lrt else -1
        print()
        print(f"  Scale:             {name}")
        print(f"  Program:           {cfg.get('program', 'unknown')}")
        print(f"  Description:       {cfg.get('description', '')}")
        print(f"  Port:              {cfg['port']}")
        print(f"  Expected IP:       {cfg.get('ip', 'any')}")
        print(f"  Connected:         {'YES' if st['connected'] else 'NO'}")
        print(f"  Records (session): {st['record_count']}")
        print(f"  Last record:       {_fmt_age(age)}")
        print(f"  Last TransID:      {st.get('last_trans_id') or 'none'}")
        print(f"  Last ACK sent:     {st.get('last_ack') or 'none'}")
        print(f"  Last error:        {st['last_error'] or 'none'}")
        print()
    else:
        print()
        print(f"  {'NAME':<22} {'PROG':<10} {'PORT':<7} {'CONN':<6} {'RECORDS':<9} {'LAST RECORD':<18} LAST ERROR")
        print(f"  {'─'*22} {'─'*10} {'─'*7} {'─'*6} {'─'*9} {'─'*18} {'─'*30}")
        for name, st in _runtime.items():
            lrt  = st["last_record_time"]
            age  = int(time.time() - lrt) if lrt else -1
            conn = "YES" if st["connected"] else "NO"
            err  = st["last_error"]
            err  = (err[:28] + "..") if len(err) > 30 else err
            prog = st["config"].get("program", "?")
            print(f"  {name:<22} {prog:<10} {st['config']['port']:<7} {conn:<6} {st['record_count']:<9} {_fmt_age(age):<18} {err}")
        print()


def cmd_list(args):
    scales = _config.get("scales", [])
    if not scales:
        print("  No scales in config.")
        return
    print()
    print(f"  {'NAME':<22} {'PROG':<10} {'PORT':<7} {'IP':<18} {'ENABLED':<8} DESCRIPTION")
    print(f"  {'─'*22} {'─'*10} {'─'*7} {'─'*18} {'─'*8} {'─'*25}")
    for s in scales:
        enabled = "yes" if s.get("enabled", True) else "NO"
        prog    = s.get("program", "?")
        print(f"  {s['name']:<22} {prog:<10} {s['port']:<7} {s.get('ip','any'):<18} {enabled:<8} {s.get('description','')}")
    print()


def cmd_enable(args):
    if not args:
        print("  Usage: enable <name>")
        return
    _set_enabled(args[0], True)


def cmd_disable(args):
    if not args:
        print("  Usage: disable <name>")
        return
    _set_enabled(args[0], False)


def _set_enabled(name: str, state: bool):
    for s in _config["scales"]:
        if s["name"] == name:
            s["enabled"] = state
            save_config(_config)
            word = "enabled" if state else "disabled"
            print(f"  '{name}' {word} in config. Run 'reload' to apply.")
            return
    print(f"  Scale '{name}' not found in config.")


def cmd_rename(args):
    if len(args) < 2:
        print("  Usage: rename <old_name> <new_name>")
        return
    old, new = args[0], args[1]
    for s in _config["scales"]:
        if s["name"] == old:
            s["name"] = new
            save_config(_config)
            print(f"  Renamed '{old}' -> '{new}' in config.")
            print(f"  Run 'reload' to update TCP listeners.")
            print(f"  NOTE: OPC UA folder rename requires full broker restart.")
            return
    print(f"  Scale '{old}' not found in config.")


def cmd_loglevel(args):
    if len(args) < 2:
        print("  Usage: loglevel <name> <DEBUG|INFO|WARNING|ERROR>")
        return
    name, level_str = args[0], args[1].upper()
    level = getattr(logging, level_str, None)
    if level is None:
        print(f"  Unknown level: '{level_str}'")
        return
    if name not in _loggers:
        print(f"  No active logger for '{name}'.")
        return
    _loggers[name].setLevel(level)
    print(f"  {name} log level -> {level_str}")


def cmd_tail(args):
    if not args:
        print("  Usage: tail <name> [lines]")
        return
    name     = args[0]
    n        = int(args[1]) if len(args) > 1 else 20
    log_dir  = _config.get("logging", {}).get("log_dir", "logs")
    log_path = Path(log_dir) / f"{name}.log"
    if not log_path.exists():
        print(f"  Log file not found: {log_path}")
        return
    with open(log_path, encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines[-n:]:
        print(" ", line, end="")
    print()


async def cmd_ack(args):
    """
    Manually build and write an ACK string to Writable, simulating what
    Ignition's tag change script does. Used for testing without Ignition.
    Builds: F#1=OK{TransactionID}{HHmmDDMMyy}
    """
    if not args:
        print("  Usage: ack <name> | ack last")
        return

    target = args[0]

    if target.lower() == "last":
        # Find scale with most recent record
        best_name = None
        best_time = None
        for n, st in _runtime.items():
            lrt = st.get("last_record_time")
            if lrt and (best_time is None or lrt > best_time):
                best_time = lrt
                best_name = n
        if not best_name:
            print("  No records received on any scale yet.")
            return
        name = best_name
    else:
        name = target

    if name not in _runtime:
        print(f"  Unknown scale: '{name}'")
        return

    trans_id = _runtime[name].get("last_trans_id", "")
    if not trans_id:
        print(f"  No TransactionID cached for '{name}' — has a record been received?")
        return

    ack_str = build_ack(trans_id)
    print(f"  Writing ACK to Writable: {ack_str}")
    await _opc_nodes[name]["Writable"].write_value(ack_str)
    print(f"  Done. Broker handler will pick it up within 100ms if connection is active.")


def cmd_certstatus(args):
    opc_cfg  = _config.get("opc", {})
    pki_dir  = Path(opc_cfg.get("pki_dir", "pki"))
    cert     = pki_dir / "broker_cert.pem"
    key      = pki_dir / "broker_key.pem"
    trusted  = pki_dir / "trusted"
    rejected = pki_dir / "rejected"

    print()
    print(f"  PKI directory:    {pki_dir.resolve()}")
    print(f"  Broker cert:      {'OK' if cert.exists() else 'MISSING — run gen_cert.py'}")
    print(f"  Broker key:       {'OK' if key.exists() else 'MISSING — run gen_cert.py'}")

    if cert.exists():
        try:
            from cryptography import x509
            c = x509.load_pem_x509_certificate(cert.read_bytes())
            print(f"  Cert subject:     {c.subject.rfc4514_string()}")
            print(f"  Cert expires:     {c.not_valid_after_utc.strftime('%Y-%m-%d')}")
            sans = c.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            print(f"  SANs:             {', '.join(str(s) for s in sans.value)}")
        except Exception as e:
            print(f"  Cert parse error: {e}")

    trusted_certs  = list(trusted.glob("*"))  if trusted.exists()  else []
    rejected_certs = list(rejected.glob("*")) if rejected.exists() else []
    print(f"  Trusted certs:    {len(trusted_certs)} file(s)")
    for f in trusted_certs:
        print(f"    - {f.name}")
    if rejected_certs:
        print(f"  Rejected/pending: {len(rejected_certs)} — move to trusted/ to approve")
        for f in rejected_certs:
            print(f"    - {f.name}")

    allow_no_sec = opc_cfg.get("allow_no_security", False)
    print(f"  NoSecurity mode:  {'ENABLED (disable before production)' if allow_no_sec else 'disabled'}")
    print()


COMMANDS = {
    "status":     (cmd_status,    False),
    "list":       (cmd_list,      False),
    "enable":     (cmd_enable,    False),
    "disable":    (cmd_disable,   False),
    "rename":     (cmd_rename,    False),
    "loglevel":   (cmd_loglevel,  False),
    "tail":       (cmd_tail,      False),
    "certstatus": (cmd_certstatus, False),
    "ack":        (cmd_ack,       True),   # True = async command
}


# Console loop

async def console_loop():
    loop = asyncio.get_event_loop()
    print("\nScale Broker running. Type 'help' for commands.\n")

    while True:
        try:
            line = await loop.run_in_executor(
                None, lambda: input("> ").strip()
            )
        except (EOFError, KeyboardInterrupt):
            print("\nShutting down...")
            loop.stop()
            break

        if not line:
            continue

        parts = line.split()
        cmd, args = parts[0].lower(), parts[1:]

        if cmd == "help":
            print(HELP_TEXT)
        elif cmd == "quit":
            print("Shutting down...")
            loop.stop()
            break
        elif cmd == "reload":
            await do_reload()
        elif cmd in COMMANDS:
            fn, is_async = COMMANDS[cmd]
            if is_async:
                await fn(args)
            else:
                fn(args)
        else:
            print(f"  Unknown command: '{cmd}'. Type 'help'.")


# Entry point

async def main():
    global _config, _opc_server, _make_logger, _broker_log

    parser = argparse.ArgumentParser(description="Scale Broker")
    parser.add_argument("--config", default=CONFIG_PATH)
    cli_args = parser.parse_args()

    # Load configuration first (uses print() since logger isn't ready yet)
    _config      = load_config(cli_args.config)

    # Initialize logger using settings from the loaded config
    _make_logger = setup_logging(_config.get("logging", {}))
    _broker_log  = _make_logger("broker")

    _broker_log.info("=" * 60)
    _broker_log.info("Scale Broker starting")
    _broker_log.info(f"Config source: {'env var SCALES_CONFIG' if os.getenv('SCALES_CONFIG') else cli_args.config}")

    enabled = [s for s in _config["scales"] if s.get("enabled", True)]
    _broker_log.info(f"Enabled scales: {len(enabled)}")

    for scale in enabled:
        prog = scale.get("program", "UNKNOWN").lower().strip()
        if prog not in ("finish", "rail"):
            _broker_log.warning(
                f"  Scale '{scale['name']}' has unknown program '{scale.get('program')}' — "
                "must be 'finish' or 'rail' (case-insensitive). Records will fail to parse."
            )
        _runtime[scale["name"]] = {
            "connected":        False,
            "record_count":     0,
            "last_record_time": None,
            "last_error":       "",
            "last_trans_id":    "",
            "last_ack":         "",
            "config":           scale,
        }

    _opc_server = await setup_opc_server(_config, enabled)
    _broker_log.info(f"OPC nodes built for: {list(_opc_nodes.keys())}")
    _broker_log.info(f"OPC UA endpoint: opc.tcp://0.0.0.0:4842/broker")

    await start_tcp_servers(enabled)
    asyncio.ensure_future(watchdog_task())

    async with _opc_server:
        # Prevent the EOFError crash when running as a background service in Balena/Docker
        if sys.stdin.isatty():
            await console_loop()
        else:
            _broker_log.info("Running in background mode (no TTY). Console disabled.")
            while True:
                await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
