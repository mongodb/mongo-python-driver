"""Sanity checks for the Evergreen config generation.

Also regression tests for the .evergreen script fixes in PYTHON-6167.

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
    """Whether the variant's task selectors match the task.

    "." selects a tag, "!" negates the token that follows, a bare token
    names a task.
    """
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
    """Check every variant selects at least one task.

    Every suite-family task is selected by at least one variant: a family
    restructured without variant wiring would silently never run
    (PYTHON-6167).
    """
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
    """Check each per-suite family identifies its suite.

    The name and TEST_NAME identify the suite, and no family task mixes
    min-deps with coverage (PYTHON-6167).
    """
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
    """Task-level pins: pr tasks pin MAX_VERSION.

    The "latest" nightly is unstable; the explicit 4.4 min-deps pin is the
    exception. Variant-merged pins are checked by
    test_pr_tasks_effectively_pin_max_version.
    """
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
    """Run every suite family at least once on PR patches, with coverage.

    A restructure cannot then silently drop a family from the patch build
    or the codecov report (PYTHON-6167).
    """
    for family in SUITE_FAMILIES:
        pinned = [
            task
            for task in all_tasks()
            if family in (task.tags or []) and "pr" in (task.tags or [])
        ]
        assert pinned, f"no pr task in the {family} family"
        covd = [task for task in pinned if task_vars(task).get("COVERAGE") == "1"]
        assert covd, f"no pr task with coverage in the {family} family"


def test_pr_tasks_effectively_pin_max_version():
    """Effective pins: every pr task that runs a server resolves to MAX_VERSION.

    A pr task without its own VERSION var inherits the selecting variant's
    version once variant expansions merge in. The explicit 4.4 min-deps
    pin is the exception.
    """
    tasks = [task for task in all_tasks() if "pr" in (task.tags or [])]
    offenders = []
    for variant in all_variants():
        variant_version = (variant.expansions or {}).get("VERSION")
        for task in tasks:
            if not variant_selects(variant, task):
                continue
            task_vars_ = task_vars(task)
            if task_vars_.get("VERSION") == "4.4" and task_vars_.get("TEST_MIN_DEPS") == "1":
                continue
            effective = task_vars_.get("VERSION") or variant_version
            if effective is None:
                # Helpers that run the stable default server, like kms-fail.
                continue
            if effective != generate_config_utils.MAX_VERSION:
                offenders.append((variant.name, task.name, effective))
    assert not offenders, f"pr tasks not effectively pinned to MAX_VERSION: {offenders}"


def test_server_version_pr_tasks_selected_once():
    """A pr server-version task must be selected only by its matching variant.

    The variant must run the server version the task pins. The expected
    family comes from create_server_version_variants(), not from the
    selections: a variant missing its PR selector would otherwise drop
    from both sides and the comparison would pass. Variants outside the
    family must not select these tasks (the sanitizer variant pins
    MAX_VERSION for its own mongod).
    """
    sv_tasks = [task for task in pr_tasks() if "server-version" in (task.tags or [])]
    sv_variants = generate_config.create_server_version_variants()
    offenders = {}
    for task in sv_tasks:
        own = task_vars(task).get("VERSION")
        selecting = [variant.name for variant in all_variants() if variant_selects(variant, task)]
        matching = [
            variant.name
            for variant in sv_variants
            if (variant.expansions or {}).get("VERSION") == own
        ]
        if selecting != matching:
            offenders[task.name] = {"selected by": selecting, "pins": own}
    assert not offenders, (
        f"server-version pr tasks not selected by the variant matching their pin: {offenders}"
    )


def test_pr_tasks_cover_version_boundaries():
    """PRs test the server version boundaries.

    The newest numbered server runs with every Python boundary, the min
    server with its min-Python floor.
    """
    for version, pythons in (
        (
            generate_config_utils.MAX_VERSION,
            (generate_config_utils.MIN_PYTHON, generate_config_utils.MAX_PYTHON),
        ),
        (generate_config_utils.MIN_VERSION, (generate_config_utils.MIN_PYTHON,)),
    ):
        tasks = [
            task
            for task in pr_tasks()
            if "server-version" in (task.tags or []) and task_vars(task).get("VERSION") == version
        ]
        seen = {task_vars(task).get("UV_PYTHON") for task in tasks}
        assert seen == set(pythons), (
            f"{version} pr tasks pin {sorted(seen)}, want {sorted(pythons)}"
        )
        assert len(tasks) >= 2, f"{version} has too few server-version pr tasks"


def test_amazon2023_no_toolchain_variant_runs_on_prs():
    """Keep the no-toolchain variant on PR patches.

    The Amazon Linux host has no Python toolchain, so its no-toolchain
    variant must keep running on PR patches (PYTHON-6167).
    """
    (variant,) = [v for v in all_variants() if "amazon2023" in v.name]
    assert "pr" in (variant.tags or [])
    selected = [task for task in all_tasks() if variant_selects(variant, task)]
    assert selected
    assert all("test-no-toolchain" in (task.tags or []) for task in selected)


def test_server_version_rotated_tasks_keep_coverage():
    """Per-commit coverage for this family rides the rotated boundary-Python tasks.

    Not the pr tasks, which run only on MAX_VERSION.
    """
    family = [task for task in all_tasks() if "server-version" in (task.tags or [])]
    cov = [task for task in family if task_vars(task).get("COVERAGE") == "1"]
    assert any("pr" not in (task.tags or []) for task in cov), (
        "the server-version family's only COVERAGE tasks are pr tasks"
    )
    pr_pythons = {
        combo: python
        for combos in generate_config.SERVER_VERSION_PR_PYTHONS.values()
        for combo, python in combos.items()
    }
    offenders = []
    for task in family:
        if "pr" in (task.tags or []):
            continue
        vars = task_vars(task)
        boundary = pr_pythons.get((vars["TOPOLOGY"], vars["AUTH"], vars["SSL"]))
        if vars.get("UV_PYTHON") == boundary and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
    assert not offenders, f"rotated server-version boundary tasks without COVERAGE: {offenders}"


def test_numpy_win32_tasks_run_on_windows_only():
    """Keep the win32 numpy tasks on the windows variant.

    They use the 32-bit toolchain (IS_WIN32) and must be selected only by
    the windows variant (PYTHON-6167).
    """
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
    """Check the run tests setup env carries TEST_CRYPT_SHARED.

    setup_tests.py reads it to wire the crypt_shared lib directory
    (PYTHON-6167).
    """
    _, cmds = generate_config.create_run_tests_func()
    setup_cmd = cmds[0]
    includes = setup_cmd.params["include_expansions_in_env"]
    assert "TEST_CRYPT_SHARED" in includes, includes
    assert "LIBMONGOCRYPT_URL" in includes, includes


def test_pr_min_deps_tasks_also_run_coverage():
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("TEST_MIN_DEPS") == "1" and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"min-deps PR tasks without COVERAGE: {offenders}"


def test_standard_family_pr_budget():
    """Patch-build PR budget for test-standard pr tasks.

    At most 5 (the pre-pin baseline), at most 3 on Python 3.14+ (the
    ubuntu22 compression filter).
    """
    tasks = [task for task in pr_tasks() if "test-standard" in (task.tags or [])]
    assert len(tasks) <= 5, f"too many test-standard PR tasks: {[t.name for t in tasks]}"
    newer = [
        task
        for task in tasks
        if any(
            tag in (task.tags or [])
            for tag in ("python-3.14", "python-3.14t", "python-3.15", "python-3.15t")
        )
    ]
    assert len(newer) <= 3, f"too many 3.14+ test-standard PR tasks: {[t.name for t in newer]}"


def test_pr_tasks_run_coverage():
    """Pr tasks that pin a server version run with COVERAGE=1.

    No-cov companions, infrastructure tasks (no VERSION), and the
    encryption tasks (their suites exceed the coverage timeout on
    macOS/Win64, PYTHON-6167) are exempt.
    """
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("VERSION") is not None
        and "test-non-standard-no-cov" not in (task.tags or [])
        and "test-encryption" not in (task.tags or [])
        and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"PR tasks without COVERAGE: {offenders}"


def test_rotated_tasks_keep_nightly_coverage():
    """Batch coverage rides the rotation independently of the pr tasks.

    Rotated "latest" tasks and max-Python auth-aws tasks keep COVERAGE=1
    (min-deps excepted).
    """
    offenders = []
    for task in all_tasks():
        tags = task.tags or []
        vars = task_vars(task)
        if vars.get("TEST_MIN_DEPS") == "1" or "pr" in tags:
            continue
        rotated_latest = "server-latest" in tags or ("ocsp-ecdsa" in tags and "latest" in tags)
        is_tested = ("test-standard" in tags and "server-latest" in tags) or (
            "test-non-standard" in tags
            and "server-latest" in tags
            and "sharded_cluster-auth-ssl" in tags
        )
        is_aws_max_python = (
            "auth-aws" in tags
            and "session-name" not in task.name
            and vars.get("UV_PYTHON") == generate_config_utils.MAX_PYTHON
        )
        if rotated_latest and is_tested and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
        if is_aws_max_python and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
    assert not offenders, f"rotated tasks without nightly COVERAGE: {offenders}"
    # The "latest" ECDSA staples task keeps its nightly coverage.
    latest_staples = [
        task.name
        for task in all_tasks()
        if "ocsp-ecdsa" in (task.tags or [])
        and "ocsp-staple" in (task.tags or [])
        and "latest" in (task.tags or [])
        and task_vars(task).get("COVERAGE") == "1"
    ]
    assert latest_staples, "no ECDSA staples task on the latest nightly with COVERAGE"
    # The 9.0 nightly OCSP tasks stay on the min-deps Python.
    offenders = [
        task.name
        for task in all_tasks()
        if "ocsp" in (task.tags or [])
        and "9.0" in (task.tags or [])
        and "pr" not in (task.tags or [])
        and task_vars(task).get("TEST_MIN_DEPS") != "1"
    ]
    assert not offenders, f"nightly 9.0 OCSP tasks not on min-deps Python: {offenders}"


def test_non_standard_cov_tasks_have_no_cov_companions():
    """Each non-standard coverage task also emits a no-cov companion.

    The macOS/Win64 encryption variants (which filter .cov) have a task to
    activate in patch builds.
    """
    no_cov = {task.name for task in all_tasks() if "test-non-standard-no-cov" in (task.tags or [])}
    for task in all_tasks():
        if "test-non-standard-no-cov" in (task.tags or []):
            assert "pr" not in (task.tags or []), f"no-cov companion tagged pr: {task.name}"
        elif "cov" in (task.tags or []) and "test-non-standard" in (task.tags or []):
            companion = task.name.removesuffix("-cov")
            assert companion in no_cov, f"missing no-cov companion for {task.name}"


def test_fuzz_task_wiring():
    """Test the fuzz task wiring.

    Two fuzz variants: the pr-tagged regression variant runs the reproducers on
    every PR; the nightly variant (which also captures the corpus) carries no
    "pr" tag, so PR patch builds never select it, while its default patchability
    keeps it manually runnable (`evergreen patch --variant fuzz-rhel8 --task fuzz`).
    """
    tasks = [task for task in all_tasks() if "fuzz" in (task.tags or [])]
    assert [task.name for task in tasks if "pr" in (task.tags or [])] == ["fuzz-regression"]
    assert [task.name for task in tasks if "pr" not in (task.tags or [])] == ["fuzz"]
    variants = generate_config.create_fuzz_variants()
    assert len(variants) == 2
    regression, nightly = variants
    assert variant_selects(regression, tasks[0])
    assert "pr" in (regression.tags or [])
    assert variant_selects(nightly, tasks[1])
    assert nightly.batchtime == generate_config_utils.BATCHTIME_DAY
    # Webhook PR patch builds only select pr-tagged variants.
    assert not nightly.tags
    # The capture subprocess bootstraps the comment-bot github app, whose
    # secrets live in AWS Secrets Manager: it needs the assumed-role
    # credentials (as resync_specs and backport-pr also pass them).
    capture = tasks[1].commands[-1]
    env_vars = capture.params["include_expansions_in_env"]
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        assert var in env_vars, f"capture subprocess missing {var}"
    # The capture no longer keys off is_patch: manual patch runs must
    # capture too, and the nightly variant never runs in a PR build.
    assert "is_patch" not in env_vars


def test_load_balancer_marker_selection(monkeypatch):
    """The load balancer tasks select the load_balancer marker explicitly.

    pytest_conf.py only grants the implicit default markers to unmarked
    tests (PYTHON-6167).
    """
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
    """Reject DRIVERS_TOOLS paths carrying command substitution.

    A hostile path must not execute at write time or when the persisted
    env.sh is sourced (PYTHON-6167).
    """
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
    """Check the crypt_shared setup persists TEST_CRYPT_SHARED.

    The encryption tests use it to decide whether to run their crypt_shared
    cases (PYTHON-6167).
    """
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
    """Check write_env escapes backslashes first.

    Substitution payloads and trailing backslashes must round-trip
    literally when the file is sourced (PYTHON-6167).
    """
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
    """Check read_env yields the value bash sourcing would.

    Double-quoted escapes unescape, single-quoted values stay literal
    (PYTHON-6167).
    """
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


def test_write_env_escapes_embedded_quotes(monkeypatch, tmp_path):
    """Check write_env escapes embedded double quotes.

    Dropping them would lose data: a value read with read_env must
    round-trip when persisted (PYTHON-6167).
    """
    import subprocess

    import utils

    source = tmp_path / "secrets-export.sh"
    source.write_text('export QUOTE="a\\"b"\n')
    env_file = tmp_path / "test-env.sh"
    monkeypatch.setattr(utils, "ENV_FILE", env_file)

    utils.write_env("QUOTE", utils.read_env(source)["QUOTE"])
    # escape=False passes the value through raw: a caller-pre-escaped quote
    # stays literal while expansions stay live.
    utils.write_env("LIVE", 'a\\"b${UNSET_VAR}', escape=False)

    result = subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            '. "$1" && printf "%s\\n" "$QUOTE" "$LIVE"',
            "bash",
            str(env_file),
        ],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        check=False,  # The file must source cleanly; asserted via returncode.
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == 'a"b\na"b\n', repr(result.stdout)


def test_crypt_shared_escapes_backslashes(monkeypatch, tmp_path):
    """Check the crypt_shared lib path persists literally.

    The trailing LD_LIBRARY_PATH expansion stays live (PYTHON-6167).
    """
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
    """PYTHON-6048: one pr-tagged sanitizer task, selected by exactly one variant.

    The variant pins MAX_VERSION and runs on a large host for ASan's
    memory headroom. VERSION stays off the task so it is exempt from the
    pr COVERAGE rule (coverage under ASan is not useful).
    """
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
