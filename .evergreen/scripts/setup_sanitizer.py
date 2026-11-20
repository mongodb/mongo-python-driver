"""Sanitizer test task configuration (PYTHON-6048, PYTHON-6168).

The sanitizer tasks build the C extensions with -fsanitize=... flags against
the toolchain's instrumented interpreters and run the files in
SANITIZER_FILES.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from utils import PLATFORM, ROOT, SANITIZER_PYTHON, TSAN_PYTHON, write_env

# Run under the sanitizer tasks (PYTHON-6048, PYTHON-6168): exercise the C
# extensions' buffer, message-building, and concurrency entry points.
SANITIZER_FILES = [
    "test/test_bson.py",
    "test/test_message.py",
    "test/test_raw_bson.py",
    "test/test_raw_bson_shared.py",
    "test/test_client.py",
]

# The clang (14.0.6) that built the toolchain's sanitizer interpreters on the
# rhel87 build host. Extensions must use the same version: a newer clang
# emits references the interpreter's runtime does not export.
CLANG = "/usr/bin/clang"
CLANGXX = "/usr/bin/clang++"


def tsan_interface_exported(python_binary: str) -> bool:
    """Check the interpreter's dynamic symbols for the TSan interface.

    Instrumented extensions resolve __tsan_func_entry and friends against
    the interpreter's dynamic exports. An interpreter built without the
    sanitizer exports none of them.
    """
    nm = shutil.which("nm")
    if nm is None:
        # No binutils: skip the check rather than fail the setup.
        return True
    out = subprocess.run(  # noqa: S603
        [nm, "-D", python_binary],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return "__tsan_func_entry" in out


def configure_env(sub_test_name: str, test_args: str) -> str:
    """Write the environment for a sanitizer test run.

    Returns the TEST_ARGS value to run the tests with.
    """
    tsan = sub_test_name == "tsan"
    python = TSAN_PYTHON if tsan else SANITIZER_PYTHON
    if PLATFORM != "linux":
        raise RuntimeError("The sanitizer tests require the Linux Python toolchain.")
    if not Path(python).is_file():
        raise RuntimeError(
            f"Sanitizer interpreter not found at {python}. Run the task on a "
            "host with the sanitizer toolchain Python."
        )
    if not Path(CLANG).is_file():
        raise RuntimeError(
            f"Compiler not found at {CLANG}: the llvm-toolset module clang "
            "that built the toolchain's sanitizer interpreters. The task "
            "image must provide it (PYTHON-6067)."
        )
    # Clear stale artifacts: build_ext reuses them by mtime (same ABI tag).
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    for pkg in ["bson", "pymongo"]:
        for ext in (ROOT / pkg).glob("*.so"):
            ext.unlink()
    write_env("CC", CLANG)
    write_env("CXX", CLANGXX)
    write_env("UV_PYTHON", python)
    write_env("PYMONGO_C_EXT_MUST_BUILD")
    # Env-var flags sit outside uv's cache key. Skip the cache so wheels
    # rebuild instrumented.
    write_env("UV_NO_CACHE", "1")
    if tsan:
        write_env("CFLAGS", "-fsanitize=thread -fno-omit-frame-pointer -g -O1")
        write_env("LDFLAGS", "-fsanitize=thread")
        if not tsan_interface_exported(python):
            raise RuntimeError(
                "The TSan interpreter does not export the TSan interface "
                "its extensions resolve against (no __tsan_func_entry in "
                "its dynamic symbols). Rebuild the toolchain (PYTHON-6067)."
            )
        suppressions = ROOT / ".evergreen" / "tsan-suppressions.txt"
        # TSan takes one suppressions file: combine CPython's free-threading
        # suppressions (shipped with the interpreter) with ours.
        toolchain_suppressions = Path(TSAN_PYTHON).parent.parent / "suppressions_free_threading.txt"
        combined = ROOT / ".evergreen" / "tsan-suppressions-combined.txt"
        with combined.open("w", encoding="utf-8") as fid:
            if toolchain_suppressions.is_file():
                fid.write(toolchain_suppressions.read_text(encoding="utf-8"))
            fid.write(suppressions.read_text(encoding="utf-8"))
        write_env(
            "TSAN_OPTIONS",
            f"halt_on_error=1:handle_segv=0:suppressions={combined}",
        )
    else:
        # -fno-sanitize=function: guard against compiler drift (PYTHON-6167).
        write_env(
            "CFLAGS",
            "-fsanitize=address,undefined -fno-sanitize=function -fno-omit-frame-pointer -g -O1",
        )
        write_env("LDFLAGS", "-fsanitize=address,undefined")
        # CPython's pymalloc trips LeakSanitizer with false positives.
        write_env("ASAN_OPTIONS", "detect_leaks=0")
        # UBSan only warns by default, so make undefined behavior fail the task.
        write_env("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
    # No LD_PRELOAD: the interpreter self-initializes its runtimes, and a
    # mismatched preloaded runtime mixes sanitizers in one process.
    # -s: with halt_on_error=1, a sanitizer failure exits before pytest replays captured stderr.
    return "-s " + " ".join(SANITIZER_FILES) + " " + test_args
