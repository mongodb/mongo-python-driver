from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from shutil import which

from utils import LOGGER, ROOT, run_command, write_env


def make_request(url, timeout=10):
    for _ in range(int(timeout)):
        try:
            urllib.request.urlopen(url)  # noqa: S310
            return
        except urllib.error.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError(f"Failed to access {url}")


def setup_mod_wsgi(sub_test_name: str) -> None:
    env = os.environ.copy()
    if sub_test_name == "embedded":
        env["MOD_WSGI_CONF"] = "mod_wsgi_test_embedded.conf"
    elif sub_test_name == "standalone":
        env["MOD_WSGI_CONF"] = "mod_wsgi_test.conf"
    else:
        raise ValueError("mod_wsgi sub test must be either 'standalone' or 'embedded'")
    write_env("MOD_WSGI_CONF", env["MOD_WSGI_CONF"])
    apache = which("apache2")
    if not apache and Path("/usr/lib/apache2/mpm-prefork/apache2").exists():
        apache = "/usr/lib/apache2/mpm-prefork/apache2"
    if apache:
        apache_config = "apache24ubuntu161404.conf"
    else:
        apache = which("httpd")
        if not apache:
            raise ValueError("Could not find apache2 or httpd")
        apache_config = "apache22amazon.conf"
    python_version = ".".join(str(val) for val in sys.version_info[:2])
    mod_wsgi_version = 4
    so_file = f"/opt/python/mod_wsgi/python_version/{python_version}/mod_wsgi_version/{mod_wsgi_version}/mod_wsgi.so"
    write_env("MOD_WSGI_SO", so_file)
    env["MOD_WSGI_SO"] = so_file
    env["PYTHONHOME"] = f"/opt/python/{python_version}"
    env["PROJECT_DIRECTORY"] = project_directory = str(ROOT)
    write_env("APACHE_BINARY", apache)
    write_env("APACHE_CONFIG", apache_config)
    uri1 = f"http://localhost:8080/interpreter1{project_directory}"
    write_env("TEST_URI1", uri1)
    uri2 = f"http://localhost:8080/interpreter2{project_directory}"
    write_env("TEST_URI2", uri2)
    run_command(f"{apache} -k start -f {ROOT}/test/mod_wsgi_test/{apache_config}", env=env)

    # Wait for the endpoints to be available.
    try:
        make_request(uri1, 10)
        make_request(uri2, 10)
    except Exception as e:
        LOGGER.error(Path("error_log").read_text())
        raise e


def test_mod_wsgi() -> None:
    sys.path.insert(0, ROOT)
    from test.mod_wsgi_test.test_client import main, parse_args

    uri1 = os.environ["TEST_URI1"]
    uri2 = os.environ["TEST_URI2"]
    args = f"-n 25000 -t 100 parallel {uri1} {uri2}"
    try:
        main(*parse_args(args.split()))

        args = f"-n 25000 serial {uri1} {uri2}"
        main(*parse_args(args.split()))
    except Exception as e:
        LOGGER.error(Path("error_log").read_text())
        raise e


def teardown_mod_wsgi() -> None:
    apache = os.environ["APACHE_BINARY"]
    apache_config = os.environ["APACHE_CONFIG"]

    run_command(f"{apache} -k stop -f {ROOT}/test/mod_wsgi_test/{apache_config}")


if __name__ == "__main__":
    setup_mod_wsgi()
