"""Sanity checks for the Evergreen config generation, and regression tests
for the .evergreen script fixes in PYTHON-6167.

Run: pytest .evergreen/scripts/test_generate_config.py -m ""
(-m "" clears the repo-wide marker filter.)
Requires the generate-config hook's dependencies: pytest, pytest-asyncio, shrub.py, pyyaml.
"""

from __future__ import annotations

import sys
from inspect import getmembers, isfunction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import generate_config
import generate_config_utils

SUITE_FAMILIES = {
    "test-encryption": "encryption",
    "test-encryption-pyopenssl": "encryption",
    "test-load-balancer": "load_balancer",
    "test-doctest": "doctest",
    "test-compression": "default",
    "test-pyopenssl": "default",
    "test-gevent": "default",
    "test-disable-test-commands": "default",
    "test-no-c-ext": "default",
    "test-stable-api": "default",
    "test-standard": "default",
    "test-storage-inmemory": "default",
}


def all_tasks():
    tasks = []
    for name, func in getmembers(generate_config, isfunction):
        if name.startswith("create_") and name.endswith("_tasks"):
            tasks.extend(func())
    return tasks


def task_vars(task):
    vars = {}
    for command in task.commands or []:
        vars.update(getattr(command, "vars", None) or {})
    return vars


def pr_tasks():
    return [task for task in all_tasks() if "pr" in (task.tags or [])]


def all_variants():
    variants = []
    for name, func in getmembers(generate_config, isfunction):
        if name.startswith("create_") and name.endswith("_variants"):
            variants.extend(func())
    return variants


def variant_selects(variant, task):
    """Whether the variant's task selectors match the task: "." selects a
    tag, "!" negates the token that follows, a bare token names a task."""
    tags = task.tags or []
    for ref in variant.tasks:
        includes, excludes, names = [], [], []
        for token in ref.name.split():
            if token.startswith("!."):
                excludes.append(token[2:])
            elif token.startswith("!"):
                excludes.append(token[1:])
            elif token.startswith("."):
                includes.append(token[1:])
            else:
                names.append(token)
        if any(tag in tags for tag in excludes):
            continue
        if not includes and not names:
            # A negation-only selector matches every remaining task.
            return True
        if names and task.name not in names:
            continue
        if includes and not all(tag in tags for tag in includes):
            continue
        return True
    return False


def test_generation_sanity():
    """The generator emits tasks and variants with unique names."""
    tasks = all_tasks()
    variants = all_variants()
    assert tasks, "the generator emits no tasks"
    assert variants, "the generator emits no variants"
    names = [task.name for task in tasks]
    assert len(names) == len(set(names)), "duplicate task names"
    names = [variant.name for variant in variants]
    assert len(names) == len(set(names)), "duplicate variant names"


def test_variants_select_tasks():
    """Every variant selects at least one task, and every suite-family task
    is selected by at least one variant: a family restructured without
    variant wiring would silently never run (PYTHON-6167)."""
    tasks = all_tasks()
    for variant in all_variants():
        assert any(variant_selects(variant, task) for task in tasks), (
            f"{variant.name} selects no tasks"
        )
    for family in SUITE_FAMILIES:
        for task in [t for t in tasks if family in (t.tags or [])]:
            assert any(variant_selects(variant, task) for variant in all_variants()), (
                f"no variant runs {task.name}"
            )


def test_suite_families():
    """Each per-suite family identifies its suite in its name and TEST_NAME,
    and no family task mixes min-deps with coverage (PYTHON-6167)."""
    for family, suite in SUITE_FAMILIES.items():
        for task in [t for t in all_tasks() if family in (t.tags or [])]:
            where = f"{family} {task.name}"
            assert task.name.startswith(family), where
            assert task_vars(task).get("TEST_NAME") == suite, where
            vars = task_vars(task)
            assert not (vars.get("TEST_MIN_DEPS") == "1" and vars.get("COVERAGE") == "1"), (
                f"{where} mixes min-deps with coverage"
            )


def test_pr_tasks_pin_max_version():
    """Task-level pins: pr tasks pin MAX_VERSION (the "latest" nightly is
    unstable); the explicit 4.4 min-deps pin is the exception."""
    offenders = [
        (task.name, task_vars(task).get("VERSION"))
        for task in pr_tasks()
        if task_vars(task).get("VERSION") not in (None, generate_config_utils.MAX_VERSION)
        and not (
            task_vars(task).get("VERSION") == "4.4" and task_vars(task).get("TEST_MIN_DEPS") == "1"
        )
    ]
    assert not offenders, f"PR tasks not pinned to MAX_VERSION: {offenders}"


def test_suite_families_have_pr_pins():
    """Every suite family runs at least one task on PR patches, with
    coverage, so a restructure cannot silently drop a family from the patch
    build or the codecov report (PYTHON-6167)."""
    for family in SUITE_FAMILIES:
        pinned = [
            task
            for task in all_tasks()
            if family in (task.tags or []) and "pr" in (task.tags or [])
        ]
        assert pinned, f"no pr task in the {family} family"
        covd = [task for task in pinned if task_vars(task).get("COVERAGE") == "1"]
        assert covd, f"no pr task with coverage in the {family} family"


def test_numpy_win32_tasks_run_on_windows_only():
    """The win32 numpy tasks use the 32-bit toolchain (IS_WIN32) and are
    selected only by the windows variant (PYTHON-6167)."""
    win32_tasks = [task for task in all_tasks() if "test-numpy-win32" in (task.tags or [])]
    assert win32_tasks, "no win32 numpy tasks"
    for task in win32_tasks:
        assert "test-numpy" not in (task.tags or []), task.name
        assert task_vars(task).get("IS_WIN32") == "1", task.name
    for variant in all_variants():
        if not variant.name.startswith("test-numpy"):
            continue
        selected = [task for task in win32_tasks if variant_selects(variant, task)]
        if variant.name == "test-numpy-windows":
            assert selected, "the windows numpy variant does not run the win32 tasks"
        else:
            assert not selected, (
                f"{variant.name} runs the win32 tasks: {[t.name for t in selected]}"
            )


def test_run_tests_env_carries_crypt_shared():
    """The run tests setup env carries TEST_CRYPT_SHARED, which
    setup_tests.py reads to wire the crypt_shared lib directory
    (PYTHON-6167)."""
    _, cmds = generate_config.create_run_tests_func()
    setup_cmd = cmds[0]
    includes = setup_cmd.params["include_expansions_in_env"]
    assert "TEST_CRYPT_SHARED" in includes, includes
    assert "LIBMONGOCRYPT_URL" in includes, includes


def test_load_balancer_marker_selection(monkeypatch):
    """The load balancer tasks select the load_balancer marker explicitly:
    pytest_conf.py only grants the implicit default markers to unmarked
    tests (PYTHON-6167)."""
    import run_tests

    monkeypatch.setattr(run_tests, "TEST_NAME", "load_balancer")
    monkeypatch.setattr(run_tests, "GREEN_FRAMEWORK", None)
    monkeypatch.setattr(run_tests, "TEST_ARGS", ["--ignore=test/asynchronous"])
    run_tests.handle_load_balancer_marker()
    assert run_tests.TEST_ARGS[-2:] == [
        "-m",
        "default or default_async or load_balancer",
    ]


def test_ensure_openssl3_escapes_persisted_values(monkeypatch, tmp_path):
    """A DRIVERS_TOOLS path carrying command substitution must not execute
    at write time or when the persisted env.sh is sourced (PYTHON-6167)."""
    import subprocess

    import setup_tests
    import utils

    monkeypatch.setattr(setup_tests, "PLATFORM", "linux")
    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)
    monkeypatch.setattr(setup_tests, "ENV_FILE", env_file)

    # A system OpenSSL older than 3.0 forces the source path.
    fake_openssl = tmp_path / "openssl"
    fake_openssl.write_text('#!/bin/sh\necho "OpenSSL 1.1.1w  11 Sep 2023"\n')
    fake_openssl.chmod(0o755)
    monkeypatch.setattr(setup_tests.shutil, "which", lambda _name: str(fake_openssl))

    # The drivers-evergreen-tools path carries a command substitution payload,
    # reflected into the exported values by the install script.
    payload_dir = tmp_path / "dr$(touch pwned.txt)vers"
    script_dir = payload_dir / ".evergreen"
    script_dir.mkdir(parents=True)
    script = script_dir / "install-openssl3.sh"
    script.write_text(f"export OPENSSL_DIR='{script_dir}/openssl3'\n")
    monkeypatch.setenv("DRIVERS_TOOLS", str(payload_dir))
    monkeypatch.setattr(setup_tests, "DRIVERS_TOOLS", str(payload_dir))

    setup_tests.ensure_openssl3()

    # The payload must not have executed at write time...
    assert not (Path.cwd() / "pwned.txt").exists()
    assert not (tmp_path / "pwned.txt").exists()
    # ...nor when the persisted environment is sourced: the values round-trip
    # literally.
    env = subprocess.run(  # noqa: S603
        ["/bin/bash", "-c", '. "$1" >/dev/null && env', "bash", str(env_file)],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(tmp_path),
    ).stdout
    assert f"OPENSSL_DIR={script_dir}/openssl3" in env
    assert not (Path.cwd() / "pwned.txt").exists()
    assert not (tmp_path / "pwned.txt").exists()


def test_crypt_shared_persisted_to_env(monkeypatch, tmp_path):
    """The crypt_shared setup persists TEST_CRYPT_SHARED to the test
    environment: the encryption tests use it to decide whether to run their
    crypt_shared cases (PYTHON-6167)."""
    import setup_tests
    import utils

    monkeypatch.setattr(setup_tests, "check_drivers_tools", lambda: None)
    monkeypatch.setattr(setup_tests, "run_command", lambda *args, **kwargs: None)
    monkeypatch.setattr(setup_tests, "ensure_openssl3", lambda: None)
    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)
    monkeypatch.setattr(setup_tests, "ENV_FILE", env_file)
    drivers_tools = tmp_path / "drivers-tools"
    mo_expansion = drivers_tools / "mo-expansion.sh"
    mo_expansion.parent.mkdir(parents=True)
    mo_expansion.write_text('CRYPT_SHARED_LIB_PATH="/crypt_shared/lib/mongo_crypt.so"\n')
    monkeypatch.setenv("DRIVERS_TOOLS", str(drivers_tools))
    monkeypatch.setattr(setup_tests, "DRIVERS_TOOLS", str(drivers_tools))
    monkeypatch.setenv("TEST_CRYPT_SHARED", "1")
    monkeypatch.setattr(sys, "argv", ["setup_tests.py", "default"])

    setup_tests.handle_test_env()

    env_lines = env_file.read_text().splitlines()
    assert 'export TEST_CRYPT_SHARED="1"' in env_lines, env_lines
    assert any(
        line.startswith("export LD_LIBRARY_PATH=") and "/crypt_shared/lib" in line
        for line in env_lines
    ), env_lines


def test_write_env_escapes_backslashes(monkeypatch, tmp_path):
    """write_env escapes backslashes first, so substitution payloads and
    trailing backslashes round-trip literally when the file is sourced
    (PYTHON-6167)."""
    import subprocess

    import utils

    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)

    utils.write_env("PAYLOAD", "\\$(touch pwned.txt)")
    utils.write_env("TRAILING", "C:\\tools\\")
    utils.write_env("TICKS", "a`echo pwned`b")

    result = subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            '. "$1" && printf "%s\\n" "$PAYLOAD" "$TRAILING" "$TICKS"',
            "bash",
            str(env_file),
        ],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        check=False,  # The file must source cleanly; asserted via returncode.
    )
    # The file must source cleanly and every value must round-trip literally.
    assert result.returncode == 0, result.stderr
    assert result.stdout == "\\$(touch pwned.txt)\nC:\\tools\\\na`echo pwned`b\n"
    assert not (Path.cwd() / "pwned.txt").exists()
    assert not (tmp_path / "pwned.txt").exists()


def test_read_env_unescapes_double_quoted_escapes(monkeypatch, tmp_path):
    """read_env yields the value bash sourcing would: double-quoted escapes
    unescape, single-quoted values stay literal (PYTHON-6167)."""
    import subprocess

    import utils

    source = tmp_path / "secrets-export.sh"
    source.write_text(
        'export DB="\\$external"\n'
        "export SINGLE='\\$external'\n"
        'export TICK="a\\`echo hi\\`b"\n'
        'export QUOTE="a\\"b"\n'
        'export BS="a\\\\b"\n'
    )
    config = utils.read_env(source)
    assert config["DB"] == "$external"
    assert config["SINGLE"] == "\\$external"
    assert config["TICK"] == "a`echo hi`b"
    assert config["QUOTE"] == 'a"b'
    assert config["BS"] == "a\\b"

    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)
    utils.write_env("SASL_DB", config["DB"])
    result = subprocess.run(  # noqa: S603
        ["/bin/bash", "-c", '. "$1" && printf "%s" "$SASL_DB"', "bash", str(env_file)],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        check=False,  # The file must source cleanly; asserted via returncode.
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "$external", repr(result.stdout)


def test_crypt_shared_escapes_backslashes(monkeypatch, tmp_path):
    """The crypt_shared lib path persists literally while the trailing
    LD_LIBRARY_PATH expansion stays live (PYTHON-6167)."""
    import subprocess

    import setup_tests
    import utils

    monkeypatch.setattr(setup_tests, "check_drivers_tools", lambda: None)
    monkeypatch.setattr(setup_tests, "run_command", lambda *args, **kwargs: None)
    monkeypatch.setattr(setup_tests, "ensure_openssl3", lambda: None)
    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)
    monkeypatch.setattr(setup_tests, "ENV_FILE", env_file)
    # The secrets service writes the path bash-escaped; read_env sees the
    # literal, so persist its escaping.
    escaped_payload = "\\$(touch cs-pwned.txt)"
    literal_payload = "$(touch cs-pwned.txt)"
    drivers_tools = tmp_path / "drivers-tools"
    mo_expansion = drivers_tools / "mo-expansion.sh"
    mo_expansion.parent.mkdir(parents=True)
    mo_expansion.write_text(f'CRYPT_SHARED_LIB_PATH="{escaped_payload}/mongo_crypt.so"\n')
    monkeypatch.setenv("DRIVERS_TOOLS", str(drivers_tools))
    monkeypatch.setattr(setup_tests, "DRIVERS_TOOLS", str(drivers_tools))
    monkeypatch.setenv("TEST_CRYPT_SHARED", "1")
    monkeypatch.setattr(sys, "argv", ["setup_tests.py", "default"])
    # A pre-set LD_LIBRARY_PATH proves the trailing expansion stays live
    # regardless of the runner's environment.
    monkeypatch.setenv("LD_LIBRARY_PATH", "/preset/lib")

    setup_tests.handle_test_env()

    result = subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            '. "$1" >/dev/null && printf "%s" "$LD_LIBRARY_PATH"',
            "bash",
            str(env_file),
        ],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        check=False,  # The file must source cleanly; asserted via returncode.
    )
    assert result.returncode == 0, result.stderr
    # The literal prefix round-trips and the trailing expansion stays live.
    assert result.stdout == f"{literal_payload}:/preset/lib", repr(result.stdout)
    assert not (Path.cwd() / "cs-pwned.txt").exists()
    assert not (tmp_path / "cs-pwned.txt").exists()


def test_sync_option_env_override(monkeypatch):
    """The SYNC env var selects the --sync option (PYTHON-6167)."""
    import utils

    monkeypatch.setattr(sys, "argv", ["setup_tests.py", "encryption"])
    monkeypatch.delenv("SYNC", raising=False)
    opts, _ = utils.get_test_options("Set up the test environment.")
    assert opts.sync is None
    monkeypatch.setenv("SYNC", "async")
    opts, _ = utils.get_test_options("Set up the test environment.")
    assert opts.sync == "async"


def test_sanitizer_task_wiring():
    """PYTHON-6048: one pr-tagged sanitizer task, selected by exactly one
    variant that pins MAX_VERSION and runs on a large host for ASan's memory
    headroom. VERSION stays off the task so it is exempt
    from the pr COVERAGE rule (coverage under ASan is not useful)."""
    tasks = [task for task in all_tasks() if "sanitizer" in (task.tags or [])]
    assert len(tasks) == 1
    task = tasks[0]
    assert "pr" in task.tags
    funcs = [getattr(cmd, "func", None) for cmd in task.commands]
    assert funcs == ["run server", "run tests"]
    vars_ = task_vars(task)
    assert vars_["TEST_NAME"] == "sanitizer"
    assert vars_["TOPOLOGY"] == "standalone"
    assert vars_["AUTH"] == "noauth"
    assert vars_["SSL"] == "nossl"
    assert "VERSION" not in vars_
    variants = [v for v in all_variants() if variant_selects(v, task)]
    assert len(variants) == 1
    assert variants[0].run_on == ["rhel8.10-large"]
    assert (variants[0].expansions or {}).get("VERSION") == generate_config_utils.MAX_VERSION
