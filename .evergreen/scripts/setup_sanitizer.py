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

# The test files run under the sanitizer tasks (PYTHON-6048, PYTHON-6168,
# PYTHON-6285). They exercise the buffer-protocol acquisition path in
# bson/_cbsonmodule.c (_get_buffer/_prepare_input_buffer), the wire-message
# builders in pymongo/_cmessagemodule.c (_op_msg, _batched_op_msg,
# _encode_batched_write_command), the hostile-input and boundary tests
# driving the decoder and builders directly, and the PYTHON-6168
# concurrency classes: concurrent zero-copy RawBSONDocument reads and
# batched message building on free-threaded CPython.
SANITIZER_FILES = [
    "test/test_bson.py",
    "test/test_message.py",
    "test/test_raw_bson.py",
    "test/test_raw_bson_shared.py",
    "test/test_client.py",
]

# The compiler that built the toolchain's sanitizer interpreters.
# build-cpython-linux.sh exports CC=clang on the toolchain's rhel87-large
# build host, where PATH clang is the RHEL 8 AppStream llvm-toolset module
# (clang 14.0.6, as the interpreters report in platform.python_compiler()).
# The rhel87-large runtime image ships the same clang-14.0.6 rpm, installed
# into the normal filesystem. Instrumented extensions must be compiled by
# the same version: a newer clang emits references the interpreter's
# runtime does not export (undefined symbol at import).
CLANG = "/usr/bin/clang"
CLANGXX = "/usr/bin/clang++"


def tsan_interface_exported(python_binary: str) -> bool:
    """Check the interpreter's dynamic symbols for the TSan interface.

    Instrumented extensions resolve __tsan_func_entry and friends against
    the interpreter's dynamic exports; an interpreter built without the
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
            f"Sanitizer interpreter not found at {python}; the task must run "
            "on a host with the sanitizer toolchain Python."
        )
    if not Path(CLANG).is_file():
        raise RuntimeError(
            f"Compiler not found at {CLANG}: the llvm-toolset module clang "
            "that built the toolchain's sanitizer interpreters. The task "
            "image must provide it (PYTHON-6067)."
        )
    # Clear stale C artifacts: build_ext reuses them by mtime, and a
    # plain same-version CPython build shares the ABI tag.
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    for pkg in ["bson", "pymongo"]:
        for ext in (ROOT / pkg).glob("*.so"):
            ext.unlink()
    write_env("CC", CLANG)
    write_env("CXX", CLANGXX)
    write_env("UV_PYTHON", python)
    write_env("PYMONGO_C_EXT_MUST_BUILD")
    # The sanitizer flags live in environment variables, which are not
    # part of uv's wheel cache key: a cached wheel built without them
    # would load uninstrumented extensions and silently void the run.
    write_env("UV_NO_CACHE", "1")
    if tsan:
        write_env("CFLAGS", "-fsanitize=thread -fno-omit-frame-pointer -g -O1")
        write_env("LDFLAGS", "-fsanitize=thread")
        # The instrumented interpreter must also export the runtime
        # interface the extensions resolve against.
        if not tsan_interface_exported(python):
            raise RuntimeError(
                "The TSan interpreter does not export the TSan interface "
                "its extensions resolve against (no __tsan_func_entry in "
                "its dynamic symbols). Rebuild the toolchain (PYTHON-6067)."
            )
        suppressions = ROOT / ".evergreen" / "tsan-suppressions.txt"
        write_env(
            "TSAN_OPTIONS",
            f"halt_on_error=1:handle_segv=0:suppressions={suppressions}",
        )
    else:
        # -fno-sanitize=function: parity with main (PYTHON-6167). With the
        # compiler pinned to the interpreter's build compiler the function
        # check resolves fine, and the flag costs nothing; it only matters
        # if the pinned compiler and the interpreter ever drift.
        write_env(
            "CFLAGS",
            "-fsanitize=address,undefined -fno-sanitize=function -fno-omit-frame-pointer -g -O1",
        )
        write_env("LDFLAGS", "-fsanitize=address,undefined")
        # CPython's pymalloc trips LeakSanitizer with false positives.
        write_env("ASAN_OPTIONS", "detect_leaks=0")
        # UBSan only warns by default; make undefined behavior fail the task.
        write_env("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
    # The toolchain interpreter self-initializes the sanitizer runtimes,
    # so no LD_PRELOAD: preloading a mismatched runtime would mix two
    # sanitizers in one process, and for TSan report false races inside
    # CPython's own free-threading internals.
    # -s: with halt_on_error=1, a sanitizer failure exits the interpreter
    # before pytest replays captured stderr, losing the report and trace.
    return "-s " + " ".join(SANITIZER_FILES) + " " + test_args
