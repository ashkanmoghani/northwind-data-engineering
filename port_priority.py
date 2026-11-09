import datetime as dt
import json
import os
import re
import socket
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PORTS = {
    "airflow-webserver": 8080,
    "northwind_kafka": 9092,
    "ops-console": 8090,
    "spark-ui": 8081,
}
VARIABLE = re.compile(r"\$\{([A-Za-z_]\w*)(?::[-?][^}]*)?\}")
SHORT = re.compile(
    r"^(?P<address>.*:)?(?P<published>\d+|\$\{[^}]+\}):(?P<target>\d+)(?P<protocol>/tcp)?$"
)


def command(args, *, capture=True, cwd=None):
    result = subprocess.run(
        args, cwd=cwd, text=True, capture_output=capture, timeout=240
    )
    if result.returncode:
        raise RuntimeError(
            "Command failed: "
            + " ".join(str(a) for a in args[:3])
            + (
                ". " + (result.stderr or "").strip().splitlines()[-1][:300]
                if capture and result.stderr.strip()
                else ""
            )
        )
    return result.stdout.strip() if capture else ""


def containers():
    ids = command(["docker", "ps", "-q"]).split()
    if not ids:
        return []
    fmt = '{"id":{{json .Id}},"name":{{json .Name}},"labels":{{json .Config.Labels}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}}}'
    return [
        json.loads(line)
        for line in command(["docker", "inspect", "--format", fmt, *ids]).splitlines()
    ]


def bindings(container):
    for internal, items in (container["ports"] or {}).items():
        target, protocol = internal.rsplit("/", 1)
        if protocol != "tcp":
            continue
        for entry in items or []:
            if entry["HostPort"]:
                yield int(entry["HostPort"]), int(target), entry["HostIp"]


def free_here(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False


def windows_ports():
    import shutil

    shell = shutil.which("powershell.exe")
    if not shell:
        return {}, set()
    ps = "$ErrorActionPreference='Stop'; $rows=@(Get-NetTCPConnection -State Listen | ForEach-Object { [pscustomobject]@{port=$_.LocalPort;pid=$_.OwningProcess} }); ConvertTo-Json -InputObject $rows -Compress"
    try:
        result = subprocess.run(
            [shell, "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True,
            text=True,
            timeout=16,
        )
        listeners = (
            {int(x["port"]): int(x["pid"]) for x in json.loads(result.stdout)}
            if result.returncode == 0
            else {}
        )
    except (ValueError, OSError, subprocess.TimeoutExpired):
        listeners = {}
    excluded = set()
    netsh = shutil.which("netsh.exe")
    if netsh:
        try:
            result = subprocess.run(
                [
                    netsh,
                    "interface",
                    "ipv4",
                    "show",
                    "excludedportrange",
                    "protocol=tcp",
                ],
                capture_output=True,
                timeout=8,
            )
            for a, b in re.findall(
                r"(?m)^\s*(\d+)\s+(\d+)\s*\*?\s*$",
                result.stdout.decode(errors="ignore"),
            ):
                excluded.update(range(int(a), int(b) + 1))
        except (OSError, subprocess.TimeoutExpired):
            pass
    return listeners, excluded


def group_owner(c):
    labels = c["labels"] or {}
    key = "com.docker.compose."
    name = labels.get(key + "project")
    service = labels.get(key + "service")
    work = Path(labels.get(key + "project.working_dir", "/")).resolve()
    home = Path.home().resolve()
    files = tuple(
        Path(f).resolve()
        for f in labels.get(key + "project.config_files", "").split(",")
        if f
    )
    candidate = labels.get(key + "project.environment_file", "")
    envs = tuple(Path(x).resolve() for x in candidate.split(",") if x) or (
        work / ".env",
    )
    allowed = work != home and (
        work.is_relative_to(home) or re.match(r"^/mnt/[a-z]/", str(work))
    )
    if not (
        name
        and service
        and files
        and allowed
        and len(envs) == 1
        and all(f.is_file() and f.is_relative_to(work) for f in files)
        and envs[0].is_relative_to(work)
    ):
        raise RuntimeError(
            "Port owner "
            + c["name"]
            + " is not a locally editable Compose service. Change its published port and rerun up."
        )
    return (name, work, files, envs[0]), service


def old_compose(owner, *args, capture=True):
    project, work, files, env = owner
    cmd = [
        "docker",
        "compose",
        "--profile",
        "*",
        "--project-directory",
        str(work),
        "-p",
        project,
    ]
    if env.is_file():
        cmd.extend(["--env-file", str(env)])
    for f in files:
        cmd.extend(["-f", str(f)])
    return command([*cmd, *args], cwd=work, capture=capture)


def yaml_parser():
    import build_inputs

    wheel = build_inputs.yaml_wheel()
    sys.path.insert(0, str(wheel))
    import yaml

    return yaml


def field(node, key):
    for k, v in (
        getattr(node, "value", []) if getattr(node, "id", "") == "mapping" else []
    ):
        if k.value == key:
            return v
    return None


def set_env(text, key, new):
    pat = re.compile(
        r"(?m)^([ \t]*(?:export[ \t]+)?" + re.escape(key) + r"[ \t]*=)[^\r\n]*$"
    )
    count = len(pat.findall(text))
    if count > 1:
        raise RuntimeError("Ambiguous old Compose setting: " + key)
    return (
        pat.sub(lambda m: m[1] + str(new), text)
        if count
        else text.rstrip("\n") + f"\n{key}={new}\n"
    )


def rewrite(owner, moves, before, read):
    yaml = yaml_parser()
    project, work, files, env = owner
    changes = {}
    edits = {}
    for service, old, target, new in moves:
        found = []
        for file in files:
            src = read(file)
            services = field(yaml.compose(src, Loader=yaml.SafeLoader), "services")
            item = field(services, service)
            ports = field(item, "ports")
            for entry in getattr(ports, "value", []):
                if entry.id == "scalar":
                    m = SHORT.fullmatch(entry.value)
                    if not m or int(m["target"]) != target:
                        continue
                    pub = m["published"]
                    rendered = (
                        (m["address"] or "")
                        + str(new)
                        + ":"
                        + m["target"]
                        + (m["protocol"] or "")
                    )
                    raw = src[entry.start_mark.index : entry.end_mark.index]
                    replacement = raw.replace(entry.value, rendered)
                    node = entry
                else:
                    node = field(entry, "published")
                    port = field(entry, "target")
                    if not node or not port or port.value != str(target):
                        continue
                    pub = node.value
                    raw = src[node.start_mark.index : node.end_mark.index]
                    replacement = raw.replace(pub, str(new))
                var = VARIABLE.fullmatch(pub)
                if pub != str(old) and not var:
                    continue
                found.append((file, node, replacement, var))
        if len(found) != 1:
            raise RuntimeError(
                f"Unable to safely locate {project}/{service} published port {old}"
            )
        file, node, replacement, var = found[0]
        if var:
            changes[env] = set_env(changes.get(env, read(env)), var[1], new)
        else:
            edits.setdefault(file, []).append(
                (node.start_mark.index, node.end_mark.index, replacement)
            )
        envmap = before["services"][service].get("environment", {})
        for key in ("KAFKA_ADVERTISED_LISTENERS", "KAFKA_CFG_ADVERTISED_LISTENERS"):
            val = envmap.get(key, "") or ""
            if not re.search(r":%s(?:,|$)" % old, val):
                continue
            if var:
                continue
            hit = False
            for conf in files:
                src = read(conf)
                item = field(
                    field(yaml.compose(src, Loader=yaml.SafeLoader), "services"),
                    service,
                )
                en = field(field(item, "environment"), key)
                if not en or en.id != "scalar":
                    continue
                a, b = en.start_mark.index, en.end_mark.index
                revised, n = re.subn(
                    r"(://[^,/'\x22\s]+:)" + str(old) + r"(?=,|['\x22]?$)",
                    lambda m: m[1] + str(new),
                    src[a:b],
                )
                if n:
                    edits.setdefault(conf, []).append((a, b, revised))
                    hit = True
                health = field(field(item, "healthcheck"), "test")
                if health and "INTERNAL://" in val and ":19092" in val:
                    a, b = health.start_mark.index, health.end_mark.index
                    raw = src[a:b]
                    revised = raw.replace(
                        "--bootstrap-server localhost:" + str(old),
                        "--bootstrap-server localhost:19092",
                    )
                    if raw != revised:
                        edits.setdefault(conf, []).append((a, b, revised))
            if not hit:
                raise RuntimeError(
                    f"Could not update Kafka advertised port for {project}/{service}"
                )
    for file, spans in edits.items():
        src = read(file)
        for a, b, replacement in sorted(set(spans), reverse=True):
            src = src[:a] + replacement + src[b:]
        changes[file] = src
    return changes


def published(cfg):
    return {
        name: sorted(
            (
                str(p.get("host_ip", "")),
                int(p["published"]),
                int(p["target"]),
                p.get("protocol", "tcp"),
            )
            for p in s.get("ports", [])
            if p.get("published")
        )
        for name, s in cfg["services"].items()
    }


def write_file(path, text, mode):
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=".classic-port-", delete=False
    ) as f:
        staged = Path(f.name)
        try:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
            staged.chmod(mode)
            os.replace(staged, path)
        finally:
            staged.unlink(missing_ok=True)


def prioritize(required=None, preview=False):
    required = set(required or PORTS.values())
    current = containers()
    listeners, excluded = windows_ports()
    occupied = {p for c in current for p, _, _ in bindings(c)}
    groups = {}
    all_moves = []
    seen = {}
    for c in current:
        for port, target, ip in bindings(c):
            if port not in required or ip not in ("0.0.0.0", "::", "127.0.0.1", ""):
                continue
            if (c["labels"] or {}).get(
                "com.docker.compose.project"
            ) == "northwind_classic":
                continue
            identity = (c["id"], port, target)
            if identity in seen:
                continue
            seen[identity] = True
            owner, service = group_owner(c)
            new = next(
                (
                    n
                    for n in range(port + 10000, port + 10200)
                    if n not in occupied | required | excluded
                    and n not in listeners
                    and free_here(n)
                ),
                None,
            )
            if not new:
                raise RuntimeError("No free alternate for " + str(port))
            occupied.add(new)
            groups.setdefault(owner, []).append((service, port, target, new))
            all_moves.append((owner[0], service, port, new))
    docker_owned = {
        p
        for c in current
        for p, _, ip in bindings(c)
        if ip in ("0.0.0.0", "::", "127.0.0.1", "")
    }
    for p in sorted(required - docker_owned):
        if p in listeners or not free_here(p) or p in excluded:
            detail = (
                f"Windows PID {listeners[p]}"
                if p in listeners
                else ("Windows reserved range" if p in excluded else "a local process")
            )
            raise RuntimeError(
                f"Port {p} is occupied by {detail}. Change that program's port and rerun up."
            )
    if not groups:
        print(
            "Classic default ports are available or already owned by Classic.",
            flush=True,
        )
        return
    original = {}

    def read(path):
        return original.setdefault(path, path.read_text() if path.exists() else "")

    proposed = {}
    snapshots = {}
    for owner, moves in groups.items():
        before = json.loads(old_compose(owner, "config", "--format", "json"))
        snapshots[owner] = before
        normalized = published(before)
        for service, old, target, new in moves:
            if not any(p == old and t == target for _, p, t, _ in normalized[service]):
                raise RuntimeError(
                    f"Running and saved ports differ: {owner[0]}/{service}"
                )
            active = [
                c
                for c in current
                if (c["labels"] or {}).get("com.docker.compose.project") == owner[0]
                and (c["labels"] or {}).get("com.docker.compose.service") == service
            ]
            if len(active) != 1:
                raise RuntimeError("Cannot remap a scaled service: " + service)
            label = active[0]["labels"].get("com.docker.compose.config-hash", "")
            if (
                label
                and old_compose(owner, "config", "--hash", service).split()[-1] != label
            ):
                raise RuntimeError(
                    f"Saved configuration differs from the running {owner[0]}/{service}. Reconcile its Compose files before changing its published port."
                )
        changes = rewrite(owner, moves, before, read)
        if proposed.keys() & changes.keys():
            raise RuntimeError("Multiple Compose projects share a configuration file")
        proposed.update(changes)
    for project, service, old, new in all_moves:
        print(f"{project}/{service}: {old} -> {new}", flush=True)
    if preview:
        return
    backup = (
        ROOT
        / ".runtime"
        / "port-moves"
        / dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    )
    backup.mkdir(parents=True, mode=0o700)
    modes = {
        p: stat.S_IMODE(p.stat().st_mode) if p.exists() else 0o600 for p in proposed
    }
    existed = {p: p.exists() for p in proposed}
    index = {str(i): str(p) for i, p in enumerate(proposed)}
    write_file(backup / "files.json", json.dumps(index, indent=2), 0o600)
    changed = []
    started = []
    try:
        for i, p in enumerate(proposed):
            if (p.read_text() if p.exists() else "") != original[p]:
                raise RuntimeError("A Compose file changed during port inspection")
            write_file(backup / str(i), original[p], 0o600)
            write_file(p, proposed[p], modes[p])
            changed.append(p)
        for owner, moves in groups.items():
            after = json.loads(old_compose(owner, "config", "--format", "json"))
            expected = published(snapshots[owner])
            for service, old, target, new in moves:
                expected[service] = sorted(
                    (ip, new if (p, t) == (old, target) else p, t, proto)
                    for ip, p, t, proto in expected[service]
                )
                for key in (
                    "KAFKA_ADVERTISED_LISTENERS",
                    "KAFKA_CFG_ADVERTISED_LISTENERS",
                ):
                    oldval = (
                        snapshots[owner]["services"][service]
                        .get("environment", {})
                        .get(key, "")
                        or ""
                    )
                    if re.search(r":%s(?:,|$)" % old, oldval) and not re.search(
                        r":%s(?:,|$)" % new,
                        after["services"][service]["environment"].get(key, ""),
                    ):
                        raise RuntimeError("Kafka advertised port did not move")
            if published(after) != expected:
                raise RuntimeError(
                    "Unexpected port changes; restoring original settings"
                )
        for owner, moves in groups.items():
            names = sorted({m[0] for m in moves})
            started.append((owner, names))
            old_compose(
                owner,
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "160",
                *names,
                capture=False,
            )
        foreign = {
            p
            for c in containers()
            if (c["labels"] or {}).get("com.docker.compose.project")
            != "northwind_classic"
            for p, _, ip in bindings(c)
            if ip in ("0.0.0.0", "::", "127.0.0.1", "")
        }
        if foreign & required:
            raise RuntimeError("A previous service still holds a requested port")
    except (Exception, KeyboardInterrupt):
        for p in reversed(changed):
            if existed[p]:
                write_file(p, original[p], modes[p])
            else:
                p.unlink(missing_ok=True)
        for owner, names in reversed(started):
            try:
                old_compose(
                    owner,
                    "up",
                    "-d",
                    "--no-deps",
                    "--no-build",
                    "--pull",
                    "never",
                    *names,
                    capture=False,
                )
            except Exception:
                print("Check restored Compose project:", owner[0], file=sys.stderr)
        raise
    print("Original port settings saved in", backup, flush=True)
