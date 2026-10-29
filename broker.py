"""
Stream Broker — Rice Lake 680 Stream TCP -> OPC UA Bridge
----------------------------------------------------------
Parses the standard Rice Lake continuous stream format from a 680 indicator,
publishes live weight/mode/status to OPC UA tags, and forwards zero/tare/mode
commands from Ignition back to the indicator on the same socket.

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
    reload                       Reload config and restart TCP listeners/connections
    loglevel <name> <LEVEL>      Set log level DEBUG/INFO/WARNING/ERROR
    tail <name> [lines]          Print last n lines of scale log (default 20)
    send <name> <COMMAND>        Send a command to the indicator (test without Ignition)
    certstatus                   Show certificate and security status
    quit                         Shut down broker
    help                         Show this help

Stream format (one frame per line):
  <STX><P><W7.><U><M><S><CR><LF>
    STX  = ASCII 0x02
    P    = polarity: space = +, '-' = -
    W7.  = 7 char weight incl. decimal point, leading spaces
    U    = units: P=Primary, S=Secondary, T=Tertiary
    M    = mode: G=Gross, N=Net, T=Tare
    S    = status: space=OK, M=Motion, O=Out of range, I=Invalid, Z=Center of zero

Commands (Ignition writes one of these to the Command tag):
  KZERO, KTARE, KGROSSNET, KGROSS, KNET, KCLRTAR
  The indicator replies on the same socket (typically "OK" or "??"); any line
  that is not a stream frame is published to CommandResponse.

Connection modes (per scale, "mode" in scales.json):
  "listen"  (default) — indicator is a TCP client and connects to the broker on "port"
  "connect"           — indicator is a TCP server; broker connects to "ip":"port"
                        and reconnects automatically
"""

import asyncio
import argparse
import json
import logging
import time
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from asyncua import Server, ua


# ── Globals ────────────────────────────────────────────────────────────────────

CONFIG_PATH = "scales.json"

DEFAULT_STREAM_TIMEOUT = 5    # seconds without any data before the connection is dropped
DEFAULT_RECONNECT      = 5    # seconds between connect attempts in "connect" mode

_runtime: dict     = {}   # { name: { connected, stream_active, frame_count, last_frame_time,
                            #           last_error, last_data, last_command, last_response,
                            #           writer, config } }
_opc_nodes: dict   = {}   # { name: { tag_name: Node } }
_tcp_servers: list = []
_connect_tasks: list = []
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


# Stream parsing
#
# Frame after STX is a fixed 11 characters:
#   [0]    polarity
#   [1:8]  weight (7 chars, right justified, may include decimal point)
#   [8]    units
#   [9]    mode
#   [10]   status
#
# Do NOT strip() the raw line before parsing — polarity and status are
# both a space in the normal case.

STX        = "\x02"
FRAME_LEN  = 11

MODES = {
    "G": "GROSS",
    "N": "NET",
    "T": "TARE",
}

STATUSES = {
    " ": "OK",
    "M": "MOTION",
    "O": "OUT_OF_RANGE",
    "I": "INVALID",
    "Z": "COZ",
}

# Commands Ignition (or the console) is allowed to send to the indicator.
# Anything else is rejected so a bad tag write can't push arbitrary
# configuration commands to the indicator.
INDICATOR_COMMANDS = {"KZERO", "KTARE", "KGROSSNET", "KGROSS", "KNET", "KCLRTAR"}


def parse_stream(raw: str, unit_labels: dict) -> dict:
    """
    Parse one Rice Lake stream frame.

    Args:
        raw:         Raw ASCII line from the indicator (may include CRLF)
        unit_labels: Optional map of unit code -> label from scales.json,
                     e.g. {"P": "LB"}. Unmapped codes are published as-is.

    Returns:
        Dict of OPC UA tag name -> value. "Weight" is omitted if the weight
        field isn't numeric (out of range / invalid frames).

    Raises:
        ValueError: wrong frame length, unknown mode/status, bad weight
    """
    # Use the last STX in case of leading garbage after a partial read
    frame = raw[raw.rindex(STX) + 1:].rstrip("\r\n")

    if len(frame) != FRAME_LEN:
        raise ValueError(f"Expected {FRAME_LEN} chars after STX, got {len(frame)}")

    polarity = frame[0]
    weight   = frame[1:8].strip()
    unit     = frame[8]
    mode     = frame[9]
    status   = frame[10]

    if polarity not in (" ", "-"):
        raise ValueError(f"Unknown polarity {polarity!r}")
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}")
    if status not in STATUSES:
        raise ValueError(f"Unknown status {status!r}")

    sign    = "-" if polarity == "-" else ""
    display = sign + weight
    data = {
        "WeightDisplay": display,
        "WeightUnits":   unit_labels.get(unit, unit),
        "Mode":          MODES[mode],
        "Status":        STATUSES[status],
        "Motion":        status == "M",
        "CenterOfZero":  status == "Z",
    }

    try:
        data["Weight"] = float(display)
    except ValueError:
        # Out of range / invalid frames may not carry a usable number
        if status not in ("O", "I"):
            raise ValueError(f"Non-numeric weight {weight!r}")

    return data


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
        ("Weight",                0.0),
        ("WeightDisplay",         ""),
        ("WeightUnits",           ""),
        ("Mode",                  ""),
        ("Status",                ""),
        ("Motion",                False),
        ("CenterOfZero",          False),
        ("Command",               ""),
        ("CommandResponse",       ""),
        ("LastCommand",           ""),
        ("Connected",             False),
        ("StreamActive",          False),
        ("FrameCount",            0),
        ("SecondsSinceLastFrame", -1),
        ("LastError",             ""),
    ]

    CLIENT_WRITABLE = {"Command"}

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


def _new_runtime(scale: dict) -> dict:
    return {
        "connected":       False,
        "stream_active":   False,
        "frame_count":     0,
        "last_frame_time": None,
        "last_error":      "",
        "last_data":       None,
        "last_command":    "",
        "last_response":   "",
        "writer":          None,
        "config":          scale,
    }


# Watchdog task - timer can be modified after testing

async def watchdog_task():
    """
    Writes SecondsSinceLastFrame and FrameCount for every active scale every 1 s.
    FrameCount is written here rather than per frame to keep OPC writes down
    at stream rate.
    """
    while True:
        await asyncio.sleep(1)
        for name, state in list(_runtime.items()):
            if name not in _opc_nodes:
                continue
            lft = state.get("last_frame_time")
            val = -1 if lft is None else int(time.time() - lft)
            try:
                await _opc_nodes[name]["SecondsSinceLastFrame"].write_value(val)
                await _opc_nodes[name]["FrameCount"].write_value(state["frame_count"])
            except Exception:
                pass


# Command sender - used by the Command tag poller and the console 'send' command

async def send_command(name: str, cmd: str, log: logging.Logger):
    """
    Validate and send a command to the indicator, CRLF terminated.
    CommandResponse is cleared first so an identical reply ("OK" twice in a
    row) still fires a tag change event in Ignition.

    Raises:
        ValueError:      command not in INDICATOR_COMMANDS
        ConnectionError: indicator not connected
    """
    cmd = cmd.strip().upper()
    if cmd not in INDICATOR_COMMANDS:
        raise ValueError(f"unknown command '{cmd}'")

    writer = _runtime[name].get("writer")
    if writer is None or writer.is_closing():
        raise ConnectionError("indicator not connected")

    nodes = _opc_nodes[name]
    await nodes["CommandResponse"].write_value("")
    writer.write(f"{cmd}\r\n".encode("ascii"))
    await writer.drain()

    _runtime[name]["last_command"] = cmd
    await nodes["LastCommand"].write_value(cmd)
    log.info(f"TX: {cmd}")


async def command_poller(name: str):
    """
    Poll the Command tag every 100ms. Ignition writes a command string,
    the broker clears the tag and sends it to the indicator. Commands written
    while the indicator is offline are rejected immediately rather than
    queued, so a stale KZERO/KTARE can't fire on the next reconnect.
    """
    log  = _make_logger(name)
    node = _opc_nodes[name]["Command"]
    while True:
        await asyncio.sleep(0.1)
        if name not in _runtime:
            continue
        try:
            cmd = await node.read_value()
            if not cmd:
                continue
            await node.write_value("")
            await send_command(name, cmd, log)
        except (ValueError, ConnectionError) as e:
            log.warning(f"Command rejected: {e}")
            await _opc_nodes[name]["CommandResponse"].write_value(f"REJECTED: {e}")
        except Exception as e:
            log.error(f"Command poller error: {e}")


# Stream session - shared by listen and connect modes

async def handle_frame(name: str, raw: str, log: logging.Logger):
    st    = _runtime[name]
    nodes = _opc_nodes[name]
    log.debug(f"RX: {raw!r}")

    try:
        data = parse_stream(raw, st["config"].get("units", {}))
    except ValueError as e:
        err = f"Parse error: {e} | raw: {raw!r}"
        # Only log on change — a bad stream would otherwise log at frame rate
        if err != st["last_error"]:
            log.error(err)
            st["last_error"] = err
            await nodes["LastError"].write_value(err)
        return

    st["frame_count"]    += 1
    st["last_frame_time"] = time.time()

    if not st["stream_active"]:
        st["stream_active"] = True
        await nodes["StreamActive"].write_value(True)
        log.info("Stream active")

    if st["last_error"]:
        st["last_error"] = ""
        await nodes["LastError"].write_value("")

    # Skip OPC writes when nothing changed (most frames on a stable scale)
    if data == st["last_data"]:
        return
    st["last_data"] = data

    for key, val in data.items():
        await nodes[key].write_value(val)


async def run_session(name: str, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter, log: logging.Logger):
    st      = _runtime[name]
    nodes   = _opc_nodes[name]
    timeout = st["config"].get("stream_timeout_seconds", DEFAULT_STREAM_TIMEOUT)

    st["connected"] = True
    st["writer"]    = writer
    await nodes["Connected"].write_value(True)
    await nodes["LastError"].write_value("")

    try:
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout)
            except asyncio.TimeoutError:
                log.warning(f"No data for {timeout}s — dropping connection")
                break
            if not line:
                break

            raw = line.decode("ascii", errors="replace")

            if STX in raw:
                await handle_frame(name, raw, log)
            elif raw.strip():
                # Anything that isn't a stream frame is a command reply ("OK" / "??")
                resp = raw.strip()
                st["last_response"] = resp
                await nodes["CommandResponse"].write_value(resp)
                log.info(f"Response to {st['last_command'] or '?'}: {resp}")

    except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError) as e:
        log.warning(f"Connection error: {e}")

    finally:
        log.info("Disconnected")
        st["connected"]     = False
        st["stream_active"] = False
        st["writer"]        = None
        st["last_data"]     = None
        try:
            await nodes["Connected"].write_value(False)
            await nodes["StreamActive"].write_value(False)
        except Exception:
            pass
        writer.close()


# Listen mode - indicator connects to the broker

def make_tcp_handler(scale: dict):
    name         = scale["name"]
    expected_ip  = scale.get("ip")
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

        if _runtime[name]["connected"]:
            log.warning(f"Rejected connection from {client_ip} — already connected")
            writer.close()
            return

        log.info(f"Connected from {client_ip}")
        await run_session(name, reader, writer, log)

    return handle_client


# Connect mode - broker connects to the indicator's TCP server port

async def connect_loop(scale: dict):
    name    = scale["name"]
    host    = scale["ip"]
    port    = scale["port"]
    delay   = scale.get("reconnect_seconds", DEFAULT_RECONNECT)
    log     = _make_logger(name)
    failing = False

    if name not in _opc_nodes:
        log.error(f"OPC nodes not ready for '{name}' — full restart required")
        return

    while True:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), 10)
        except (OSError, asyncio.TimeoutError) as e:
            # Log the first failure only, not every retry
            if not failing:
                log.warning(f"Connect to {host}:{port} failed ({e!r}) — retrying every {delay}s")
                failing = True
            await asyncio.sleep(delay)
            continue

        failing = False
        log.info(f"Connected to {host}:{port}")
        await run_session(name, reader, writer, log)
        await asyncio.sleep(delay)


# TCP server / connection management

async def start_scale_io(scales: list):
    for srv in _tcp_servers:
        srv.close()
        await srv.wait_closed()
    _tcp_servers.clear()

    for task in _connect_tasks:
        task.cancel()
    _connect_tasks.clear()

    for scale in scales:
        mode = scale.get("mode", "listen").lower().strip()
        if mode == "connect":
            if not scale.get("ip"):
                _broker_log.error(f"  {scale['name']}: mode 'connect' requires 'ip' — skipped")
                continue
            _connect_tasks.append(asyncio.ensure_future(connect_loop(scale)))
            _broker_log.info(f"  {scale['name']} [connect] -> {scale['ip']}:{scale['port']}")
        else:
            handler = make_tcp_handler(scale)
            srv     = await asyncio.start_server(handler, "0.0.0.0", scale["port"])
            _tcp_servers.append(srv)
            _broker_log.info(
                f"  {scale['name']} [listen] "
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
            _runtime[name] = _new_runtime(scale)
            if name not in _opc_nodes:
                _broker_log.warning(
                    f"  '{name}' is new — OPC UA folder requires full restart to appear."
                )
        else:
            _runtime[name]["config"] = scale

    await start_scale_io(enabled)
    _broker_log.info("Reload complete.")
    return True


# Console Commands

HELP_TEXT = """
Stream Broker Management Console
─────────────────────────────────────────────────────────────────
  status                        All scales status summary
  status <name>                 Detail view for one scale
  list                          All scales from config
  enable  <name>                Enable scale in config
  disable <name>                Disable scale in config
  rename  <old> <name>          Rename scale in config
  reload                        Reload config + restart TCP listeners/connections
  loglevel <name> <LEVEL>       Set log level (DEBUG/INFO/WARNING/ERROR)
  tail <name> [lines]           Last n log lines for a scale (default 20)
  send <name> <COMMAND>         Send command to indicator
                                (KZERO KTARE KGROSSNET KGROSS KNET KCLRTAR)
  certstatus                    Show certificate and security status
  quit                          Shut down broker
  help                          Show this help
─────────────────────────────────────────────────────────────────
Note: enable / disable / rename take effect after 'reload'.
      OPC UA folder renames require a full broker restart.
      'loglevel <name> DEBUG' logs every raw stream frame.
"""


def _fmt_age(secs) -> str:
    if secs is None or secs < 0:
        return "never"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s ago"
    return f"{secs // 3600}h {(secs % 3600) // 60}m ago"


def _fmt_weight(st: dict) -> str:
    d = st.get("last_data")
    if not d:
        return "-"
    return f"{d['WeightDisplay']} {d['WeightUnits']} {d['Mode']} {d['Status']}"


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
        lft = st["last_frame_time"]
        age = int(time.time() - lft) if lft else -1
        print()
        print(f"  Scale:             {name}")
        print(f"  Mode:              {cfg.get('mode', 'listen')}")
        print(f"  Description:       {cfg.get('description', '')}")
        print(f"  Port:              {cfg['port']}")
        print(f"  IP:                {cfg.get('ip', 'any')}")
        print(f"  Connected:         {'YES' if st['connected'] else 'NO'}")
        print(f"  Stream active:     {'YES' if st['stream_active'] else 'NO'}")
        print(f"  Frames (session):  {st['frame_count']}")
        print(f"  Last frame:        {_fmt_age(age)}")
        print(f"  Current reading:   {_fmt_weight(st)}")
        print(f"  Last command:      {st.get('last_command') or 'none'}")
        print(f"  Last response:     {st.get('last_response') or 'none'}")
        print(f"  Last error:        {st['last_error'] or 'none'}")
        print()
    else:
        print()
        print(f"  {'NAME':<22} {'MODE':<8} {'PORT':<7} {'CONN':<6} {'STREAM':<7} {'READING':<28} LAST ERROR")
        print(f"  {'─'*22} {'─'*8} {'─'*7} {'─'*6} {'─'*7} {'─'*28} {'─'*30}")
        for name, st in _runtime.items():
            conn   = "YES" if st["connected"] else "NO"
            stream = "YES" if st["stream_active"] else "NO"
            err    = st["last_error"]
            err    = (err[:28] + "..") if len(err) > 30 else err
            mode   = st["config"].get("mode", "listen")
            print(f"  {name:<22} {mode:<8} {st['config']['port']:<7} {conn:<6} {stream:<7} {_fmt_weight(st):<28} {err}")
        print()


def cmd_list(args):
    scales = _config.get("scales", [])
    if not scales:
        print("  No scales in config.")
        return
    print()
    print(f"  {'NAME':<22} {'MODE':<8} {'PORT':<7} {'IP':<18} {'ENABLED':<8} DESCRIPTION")
    print(f"  {'─'*22} {'─'*8} {'─'*7} {'─'*18} {'─'*8} {'─'*25}")
    for s in scales:
        enabled = "yes" if s.get("enabled", True) else "NO"
        mode    = s.get("mode", "listen")
        print(f"  {s['name']:<22} {mode:<8} {s['port']:<7} {s.get('ip') or 'any':<18} {enabled:<8} {s.get('description','')}")
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


async def cmd_send(args):
    """
    Send a command straight to the indicator, bypassing the Command tag.
    The reply shows up in the log and on the CommandResponse tag.
    """
    if len(args) < 2:
        print("  Usage: send <name> <COMMAND>")
        print(f"  Commands: {' '.join(sorted(INDICATOR_COMMANDS))}")
        return

    name = args[0]
    if name not in _runtime or name not in _opc_nodes:
        print(f"  Unknown scale: '{name}'")
        return

    try:
        await send_command(name, args[1], _make_logger(name))
        print(f"  Sent {args[1].upper()}. Reply will appear in the log / CommandResponse tag.")
    except (ValueError, ConnectionError) as e:
        print(f"  Not sent: {e}")


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
    "send":       (cmd_send,      True),   # True = async command
}


# Console loop

async def console_loop():
    """Returns when the user quits. Shutdown happens by returning from main(),
    not loop.stop() — stopping the loop under asyncio.run() raises RuntimeError."""
    loop = asyncio.get_event_loop()
    print("\nStream Broker running. Type 'help' for commands.\n")

    while True:
        try:
            line = await loop.run_in_executor(
                None, lambda: input("> ").strip()
            )
        except EOFError:
            # stdin looked like a TTY but has no input (VS Code debugger,
            # Windows NUL device) — keep the broker running without a console
            _broker_log.info("Console input closed — continuing in background mode.")
            while True:
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            print("\nShutting down...")
            return

        if not line:
            continue

        parts = line.split()
        cmd, args = parts[0].lower(), parts[1:]

        if cmd == "help":
            print(HELP_TEXT)
        elif cmd == "quit":
            print("Shutting down...")
            return
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

    parser = argparse.ArgumentParser(description="Stream Broker")
    parser.add_argument("--config", default=CONFIG_PATH)
    cli_args = parser.parse_args()

    # Load configuration first (uses print() since logger isn't ready yet)
    _config      = load_config(cli_args.config)

    # Initialize logger using settings from the loaded config
    _make_logger = setup_logging(_config.get("logging", {}))
    _broker_log  = _make_logger("broker")

    _broker_log.info("=" * 60)
    _broker_log.info("Stream Broker starting")
    _broker_log.info(f"Config source: {'env var SCALES_CONFIG' if os.getenv('SCALES_CONFIG') else cli_args.config}")

    enabled = [s for s in _config["scales"] if s.get("enabled", True)]
    _broker_log.info(f"Enabled scales: {len(enabled)}")

    for scale in enabled:
        mode = scale.get("mode", "listen").lower().strip()
        if mode not in ("listen", "connect"):
            _broker_log.warning(
                f"  Scale '{scale['name']}' has unknown mode '{scale.get('mode')}' — "
                "must be 'listen' or 'connect'. Defaulting to listen."
            )
        _runtime[scale["name"]] = _new_runtime(scale)

    _opc_server = await setup_opc_server(_config, enabled)
    _broker_log.info(f"OPC nodes built for: {list(_opc_nodes.keys())}")
    _broker_log.info(f"OPC UA endpoint: opc.tcp://0.0.0.0:4842/broker")

    await start_scale_io(enabled)
    asyncio.ensure_future(watchdog_task())
    for name in _opc_nodes:
        asyncio.ensure_future(command_poller(name))

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
