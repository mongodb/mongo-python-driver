"""Sanitizer test task configuration (PYTHON-6048, PYTHON-6168).

The sanitizer tasks build the C extensions with -fsanitize=... flags against
the toolchain's instrumented interpreters and run the files in
SANITIZER_FILES.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

from utils import PLATFORM, ROOT, SANITIZER_PYTHON, TSAN_PYTHON, write_env

# The test files run under the sanitizer tasks (PYTHON-6048, PYTHON-6168,
# PYTHON-6285). They exercise the buffer-protocol acquisition path in
# bson/_cbsonmodule.c (_get_buffer/_prepare_input_buffer), the wire-message
# builders in pymongo/_cmessagemodule.c (_op_msg, _batched_op_msg,
# _encode_batched_write_command), the hostile-input and boundary tests
# driving the decoder and builders directly, and — via the concurrency
# classes added in PYTHON-6168 — concurrent zero-copy RawBSONDocument reads
# and batched message building on free-threaded CPython.
SANITIZER_FILES = [
    "test/test_bson.py",
    "test/test_message.py",
    "test/test_raw_bson.py",
    "test/test_raw_bson_shared.py",
    "test/test_client.py",
]


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


def _first_line(args: list[str]) -> str | None:
    """Return the first stdout line of a command, or None if it cannot run."""
    try:
        out = subprocess.run(  # noqa: S603
            args,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return out.strip().splitlines()[0] if out.strip() else None


def interpreter_clang_version(python_binary: str) -> str | None:
    """Return the clang version that built the interpreter, if discoverable.

    platform.python_compiler() reports e.g. "Clang 14.0.6 (Red Hat
    14.0.6-1.module+el8.7.0+15816+ec020e8f)" for the toolchain interpreters.
    """
    out = _first_line([python_binary, "-c", "import platform; print(platform.python_compiler())"])
    match = re.search(r"Clang (\d+(?:\.\d+)+)", out or "")
    return match.group(1) if match else None


def compiler_clang_version(compiler: str) -> str | None:
    """Return the clang version of the compiler, if discoverable."""
    match = re.search(r"clang version (\d+(?:\.\d+)+)", _first_line([compiler, "--version"]) or "")
    return match.group(1) if match else None


def clang_candidates() -> list[str]:
    """Return the clang compilers the image may carry.

    The hosts carry several: the mongodbtoolchain's versioned installs and
    the system clang, whose /usr/bin one (clang 22.1.8) is far newer than
    the module clang 14.0.6 that built the instrumented interpreters.
    """
    root = Path("/")
    return sorted(
        [
            str(path)
            for pattern in (
                "opt/mongodbtoolchain/*/bin/clang",
                "usr/lib64/llvm*/bin/clang",
                "usr/bin/clang-*",
            )
            for path in root.glob(pattern)
        ]
        + ["/usr/bin/clang", "/usr/local/bin/clang"]
    )


def find_matching_clang(version: str) -> tuple[str, str, bool]:
    """Return a clang compatible with the interpreter's build compiler.

    Instrumented extensions resolve the sanitizer's interface symbols
    against the interpreter's runtime; a NEWER compiler emits references
    the runtime does not export (undefined symbol at import: __tsan_memcpy
    under TSan with a newer clang's memcall lowering,
    __ubsan_handle_function_type_mismatch under UBSan's function check).
    The reverse direction is the supported one: the runtime's
    compiled-code interface is forward-compatible, so when the exact
    compiler is absent the newest clang OLDER than the interpreter's is a
    usable fallback. Returns (path, clang version, exact match).
    """
    probed = []
    fallback: tuple[str, str] | None = None
    required = _version_key(version)
    for candidate in clang_candidates():
        candidate_version = compiler_clang_version(candidate)
        if candidate_version is None:
            continue
        probed.append(f"{candidate} (clang {candidate_version})")
        if candidate_version == version:
            return candidate, candidate_version, True
        candidate_key = _version_key(candidate_version)
        if candidate_key < required and (
            fallback is None or candidate_key > _version_key(fallback[1])
        ):
            fallback = (candidate, candidate_version)
    if fallback is not None:
        return *fallback, False
    raise RuntimeError(
        f"No clang compatible with the interpreter's build compiler "
        f"(clang {version}) was found on this host. Probed: "
        + ("; ".join(probed) if probed else "none")
        + ". The toolchain image must provide the clang that built its "
        "instrumented interpreters (PYTHON-6067)."
    )


def _version_key(version: str) -> tuple[int, ...]:
    """Return a numeric sort key for a dotted version string."""
    return tuple(int(part) for part in version.split("."))


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
    # Clear stale C artifacts: build_ext reuses them by mtime, and a
    # plain same-version CPython build shares the ABI tag.
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    for pkg in ["bson", "pymongo"]:
        for ext in (ROOT / pkg).glob("*.so"):
            ext.unlink()
    # The extensions must be compiled by a clang whose version matches the
    # one that built the interpreter (see find_matching_clang), so resolve
    # the compiler by version match and fail setup — with the probed
    # inventory in the message — when the host has no match, rather than
    # failing deep in the run with a confusing ImportError.
    interpreter_version = interpreter_clang_version(python)
    if interpreter_version is None:
        # Cannot probe the interpreter: fall back to the system clang and
        # say so; a mismatch would surface at import time as before.
        compiler = "/usr/bin/clang"
        compiler_version = compiler_clang_version(compiler)
        print(
            "sanitizer setup: the interpreter's clang version is not "
            f"discoverable; falling back to the extension compiler {compiler} "
            f"(clang {compiler_version}); the compiler/interpreter match is "
            "unchecked."
        )
    else:
        compiler, compiler_version, exact = find_matching_clang(interpreter_version)
        if exact:
            print(
                f"sanitizer setup: extensions compile with {compiler} (clang "
                f"{compiler_version}); the interpreter was built with clang "
                f"{interpreter_version}."
            )
        else:
            print(
                f"sanitizer setup: no clang {interpreter_version} on this host; "
                f"extensions compile with {compiler} (clang {compiler_version}), "
                "the newest clang older than the interpreter's build compiler "
                "— a newer compiler emits references the interpreter's runtime "
                "does not export. Upgrade the toolchain image to ship the "
                "interpreter's compiler (PYTHON-6067)."
            )
    compiler_path = Path(compiler)
    write_env("CC", compiler)
    write_env(
        "CXX", str(compiler_path.with_name(compiler_path.name.replace("clang", "clang++", 1)))
    )
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
        # -fno-sanitize=function: the toolchain interpreter's UBSan runtime
        # predates the image's clang, whose function check references a
        # handler the runtime does not export — the extensions fail to
        # import with undefined symbol: __ubsan_handle_function_type_mismatch
        # (PYTHON-6167).
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
