"""Setup, run, and tear down the mod_wsgi tests.

Usage:
    python .evergreen/scripts/mod_wsgi_tester.py setup <standalone|embedded>
    python .evergreen/scripts/mod_wsgi_tester.py test
    python .evergreen/scripts/mod_wsgi_tester.py teardown

The commands run as separate workflow steps, so state is passed between them
in a 0600 file in a user-owned cache directory.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOGGER = logging.getLogger("mod_wsgi_tester")

ROOT = Path(__file__).parents[2]
# State lives in a user-owned cache directory rather than shared /tmp: another
# local user must not be able to read or plant it, since teardown feeds its
# values into the commands that stop Apache.
STATE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "pymongo-mod-wsgi"
STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"
PORT = 8080


def run_command(
    cmd: str | list[str], env: dict[str, str] | None = None, check: bool = True
) -> None:
    LOGGER.info(f"Running command: {cmd}")
    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            shell=isinstance(cmd, str),
            env={**os.environ, **(env or {})},
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        # A missing binary is a failed command like any other: raised when
        # required, logged and swallowed for best-effort calls. The shell
        # path used to behave the same way via the exit code, and hosts like
        # the Fedora job's container do not ship pkill.
        if check:
            raise
        LOGGER.warning(f"Command not found: {cmd if isinstance(cmd, str) else cmd[0]}")
        return
    if result.stdout:
        LOGGER.info(result.stdout)
    if result.returncode != 0:
        LOGGER.error(result.stderr)
        if check:
            raise RuntimeError(f"Command failed with code {result.returncode}: {cmd}")


def make_request(url: str, timeout: int = 10) -> None:
    for _ in range(timeout):
        try:
            # Per-request timeout, so an Apache that accepts the connection
            # but never answers fails after roughly timeout seconds instead
            # of hanging forever.
            urllib.request.urlopen(url, timeout=1)  # noqa: S310
            return
        except OSError:
            # URLError (and its HTTPError subclass) plus a socket timeout.
            pass
        time.sleep(1)
    raise TimeoutError(f"Failed to access {url}")


def find_mod_wsgi_so() -> str:
    # The .so is built against the installing interpreter and lives in the
    # installed package.
    import mod_wsgi

    so_files = list((Path(mod_wsgi.__file__).parent / "server").glob("mod_wsgi*.so"))
    if len(so_files) != 1:
        raise ValueError(f"Expected one mod_wsgi.so in {mod_wsgi.__file__}, found {so_files}")
    return str(so_files[0])


def find_apache() -> tuple[str, str]:
    """Return the Apache binary and the config file matching its distribution."""
    from shutil import which

    apache = which("apache2")
    if not apache:
        # apache2 lives in sbin, which is not always on PATH for non-root users.
        for candidate in ("/usr/lib/apache2/mpm-prefork/apache2", "/usr/sbin/apache2"):
            if Path(candidate).exists():
                apache = candidate
                break
    if apache:
        return apache, "apache24ubuntu.conf"
    apache = which("httpd")
    if not apache:
        for candidate in ("/usr/sbin/httpd", "/usr/local/apache2/bin/httpd"):
            if Path(candidate).exists():
                apache = candidate
                break
    if not apache:
        raise ValueError("Could not find apache2 or httpd")
    return apache, "httpd24fedora.conf"


def port_is_open() -> bool:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
        return True
    except OSError:
        return False


def pid_file(apache_config: str) -> Path:
    # The test configs write the master process's pid to the checkout root.
    return ROOT / ("httpd.pid" if "httpd" in apache_config else "apache2.pid")


def pid_is_our_apache(pid: int, apache_config: str) -> bool:
    # A stale pid file may name a pid the OS has reused for an unrelated
    # process, so only signal an Apache whose command line carries this
    # checkout's config argument.
    try:
        cmdline = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return False
    argv = cmdline.decode("utf-8", "replace").split("\0")
    return f"{ROOT}/test/mod_wsgi_test/{apache_config}" in argv


def stop_apache(apache: str, apache_config: str, env: dict[str, str] | None = None) -> None:
    # Idempotent: safe to call when Apache is not running.
    # Prefer the pid file: -k stop re-parses the config, which fails when the
    # mod_wsgi .so or the config's env vars are unavailable. Fall back to
    # pkill for a server whose pid file was removed under it. The commands run
    # as argument lists, so state-derived values never reach a shell.
    pid = pid_file(apache_config)
    if pid.exists():
        try:
            victim = int(pid.read_text().strip())
        except ValueError:
            victim = 0
        if victim and pid_is_our_apache(victim, apache_config):
            try:
                os.kill(victim, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                LOGGER.warning("Could not stop Apache via the pid file")
        else:
            # Stale, reused, or unreadable pid: remove the file and stop
            # through the config-validated path instead.
            LOGGER.warning(f"Removing stale pid file {pid}")
            pid.unlink()
    if not pid.exists():
        run_command(
            [apache, "-k", "stop", "-f", f"{ROOT}/test/mod_wsgi_test/{apache_config}"],
            env=env,
            check=False,
        )
    if port_is_open():
        run_command(["pkill", "-f", f"test/mod_wsgi_test/{apache_config}"], check=False)

    # Stopping returns before the port is released, which the next mode needs
    # to rebind.
    deadline = time.time() + 10
    while time.time() < deadline:
        if not port_is_open():
            return
        time.sleep(0.5)
    raise ValueError(f"Apache did not release port {PORT}")


def write_state(state: dict) -> None:
    # The cache directory is user-only (0700) and the file is created 0600, so
    # no other local user can read or plant this state.
    fd = os.open(STATE_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(state, f)


def read_state() -> dict:
    # Lost or corrupt state falls back to discovery in teardown.
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {}


def setup(sub_test_name: str) -> None:
    if sub_test_name == "embedded":
        conf = "mod_wsgi_test_embedded.conf"
    elif sub_test_name == "standalone":
        conf = "mod_wsgi_test.conf"
    else:
        raise ValueError("mod_wsgi sub test must be either 'standalone' or 'embedded'")
    apache, apache_config = find_apache()
    # On Fedora/RHEL, httpd embeds the interpreter mod_wsgi was built with and
    # needs to be told where the Python installation lives.
    env = {
        "MOD_WSGI_CONF": conf,
        "MOD_WSGI_SO": find_mod_wsgi_so(),
        "MOD_WSGI_PYTHON_HOME": sys.base_prefix,
        "PROJECT_DIRECTORY": str(ROOT),
    }
    # Stop any server left behind by a previous failed run so the port is free.
    stop_apache(apache, apache_config, env)
    run_command(
        [apache, "-k", "start", "-f", f"{ROOT}/test/mod_wsgi_test/{apache_config}"],
        env=env,
    )
    write_state({"apache": apache, "config": apache_config, "env": env})

    try:
        for interp in ("interpreter1", "interpreter2"):
            make_request(f"http://localhost:{PORT}/{interp}{ROOT}", 10)
    except Exception:
        # The Apache error log holds the actual failure. Stop the server so a
        # failed setup does not leave it bound to the port.
        stop_apache(apache, apache_config, env)
        error_log = Path("error_log")
        if error_log.exists():
            LOGGER.error(error_log.read_text())
        raise


def test() -> None:
    sys.path.insert(0, str(ROOT))
    from test.mod_wsgi_test.test_client import main, parse_args

    uri1 = f"http://localhost:{PORT}/interpreter1{ROOT}"
    uri2 = f"http://localhost:{PORT}/interpreter2{ROOT}"
    try:
        main(*parse_args(f"-n 25000 -t 100 parallel {uri1} {uri2}".split()))
        main(*parse_args(f"-n 25000 serial {uri1} {uri2}".split()))
    except BaseException:
        # The test client raises KeyboardInterrupt in the failing path.
        error_log = Path("error_log")
        if error_log.exists():
            LOGGER.error(error_log.read_text())
        raise


def teardown() -> None:
    state = read_state()
    apache = state.get("apache")
    apache_config = state.get("config")
    env = state.get("env")
    if not apache or not apache_config:
        # State lost; fall back to discovery to stop a leftover server. The
        # pid file in stop_apache usually makes this unnecessary.
        try:
            apache, apache_config = find_apache()
        except ValueError:
            return
        env = {
            "MOD_WSGI_CONF": "mod_wsgi_test.conf",
            "MOD_WSGI_PYTHON_HOME": sys.base_prefix,
            "PROJECT_DIRECTORY": str(ROOT),
        }
        try:
            env["MOD_WSGI_SO"] = find_mod_wsgi_so()
        except (ImportError, ValueError):
            pass
    stop_apache(apache, apache_config, env)


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "setup":
        setup(sys.argv[2])
    elif command == "test":
        test()
    elif command == "teardown":
        teardown()
    else:
        raise SystemExit(f"Usage: {sys.argv[0]} setup <mode>|test|teardown")
