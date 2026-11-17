# Copyright 2024-Present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Synchronization of asynchronous modules.

Used as part of our build system to generate synchronous code.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from unasync import Rule, unasync_files  # type: ignore[import-untyped,import-not-found]

MANIFEST = ".synchro-modified"
STAGING = ".synchro-staging"

replacements = {
    "AsyncCollection": "Collection",
    "AsyncDatabase": "Database",
    "_AsyncCursorBase": "_CursorBase",
    "AsyncCursor": "Cursor",
    "AsyncMongoClient": "MongoClient",
    "AsyncCommandCursor": "CommandCursor",
    "AsyncRawBatchCursor": "RawBatchCursor",
    "AsyncRawBatchCommandCursor": "RawBatchCommandCursor",
    "AsyncClientSession": "ClientSession",
    "_AsyncBoundSessionContext": "_BoundSessionContext",
    "AsyncChangeStream": "ChangeStream",
    "AsyncCollectionChangeStream": "CollectionChangeStream",
    "AsyncDatabaseChangeStream": "DatabaseChangeStream",
    "AsyncClusterChangeStream": "ClusterChangeStream",
    "_AsyncBulk": "_Bulk",
    "_AsyncClientBulk": "_ClientBulk",
    "AsyncConnection": "Connection",
    "async_command": "command",
    "async_receive_message": "receive_message",
    "async_receive_data": "receive_data",
    "async_sendall": "sendall",
    "async_socket_sendall": "sendall",
    "asynchronous": "synchronous",
    "Asynchronous": "Synchronous",
    "AsyncBulkTestBase": "BulkTestBase",
    "AsyncBulkAuthorizationTestBase": "BulkAuthorizationTestBase",
    "anext": "next",
    "aiter": "iter",
    "_ALock": "_Lock",
    "_ACondition": "_Condition",
    "AsyncGridFS": "GridFS",
    "AsyncGridFSBucket": "GridFSBucket",
    "AsyncGridIn": "GridIn",
    "AsyncGridOut": "GridOut",
    "AsyncGridOutCursor": "GridOutCursor",
    "AsyncGridOutIterator": "GridOutIterator",
    "_AsyncGridOutChunkIterator": "GridOutChunkIterator",
    "_a_grid_in_property": "_grid_in_property",
    "_a_grid_out_property": "_grid_out_property",
    "AsyncClientEncryption": "ClientEncryption",
    "AsyncMongoCryptCallback": "MongoCryptCallback",
    "AsyncKMSConnectCallback": "KMSConnectCallback",
    "AsyncHTTPProxyKMSConnect": "HTTPProxyKMSConnect",
    "AsyncExplicitEncrypter": "ExplicitEncrypter",
    "AsyncAutoEncrypter": "AutoEncrypter",
    "AsyncContextManager": "ContextManager",
    "AsyncClientContext": "ClientContext",
    "AsyncTestCollection": "TestCollection",
    "AsyncIntegrationTest": "IntegrationTest",
    "AsyncPyMongoTestCase": "PyMongoTestCase",
    "AsyncMockClientTest": "MockClientTest",
    "async_client_context": "client_context",
    "async_setup": "setup",
    "asyncSetUp": "setUp",
    "asyncTearDown": "tearDown",
    "async_teardown": "teardown",
    "pytest_asyncio": "pytest",
    "async_wait_until": "wait_until",
    "addAsyncCleanup": "addCleanup",
    "async_setup_class": "setup_class",
    "IsolatedAsyncioTestCase": "TestCase",
    "AsyncUnitTest": "UnitTest",
    "AsyncMockClient": "MockClient",
    "AsyncSpecRunner": "SpecRunner",
    "AsyncTransactionsBase": "TransactionsBase",
    "async_get_pool": "get_pool",
    "async_rs_or_single_client": "rs_or_single_client",
    "async_rs_or_single_client_noauth": "rs_or_single_client_noauth",
    "async_rs_client": "rs_client",
    "async_single_client": "single_client",
    "async_from_client": "from_client",
    "aclosing": "closing",
    "asyncAssertRaisesExactly": "assertRaisesExactly",
    "get_async_mock_client": "get_mock_client",
    "aconnect": "_connect",
    "async-transactions-ref": "transactions-ref",
    "async-snapshot-reads-ref": "snapshot-reads-ref",
    "default_async": "default",
    "aclose": "close",
    "PyMongo|async": "PyMongo",
    "PyMongo|c|async": "PyMongo|c",
    "AsyncTestGridFile": "TestGridFile",
    "AsyncTestGridFileNoConnect": "TestGridFileNoConnect",
    "AsyncTestSpec": "TestSpec",
    "AsyncSpecTestCreator": "SpecTestCreator",
    "async_set_fail_point": "set_fail_point",
    "async_ensure_all_connected": "ensure_all_connected",
    "async_repl_set_step_down": "repl_set_step_down",
    "AsyncPeriodicExecutor": "PeriodicExecutor",
    "async_wait_for_event": "wait_for_event",
    "pymongo_server_monitor_task": "pymongo_server_monitor_thread",
    "pymongo_server_rtt_task": "pymongo_server_rtt_thread",
    "_async_create_lock": "_create_lock",
    "_async_create_condition": "_create_condition",
    "_async_cond_wait": "_cond_wait",
    "AsyncNetworkingInterface": "NetworkingInterface",
    "_configured_protocol_interface": "_configured_socket_interface",
    "_async_configured_socket": "_configured_socket",
    "_async_wrap_socket_tls": "_wrap_socket_tls",
    "SpecRunnerTask": "SpecRunnerThread",
    "AsyncMockConnection": "MockConnection",
    "AsyncMockPool": "MockPool",
    "StopAsyncIteration": "StopIteration",
    "create_async_event": "create_event",
    "async_create_barrier": "create_barrier",
    "async_barrier_wait": "barrier_wait",
    "async_joinall": "joinall",
    "async_simple_test_client": "simple_test_client",
    "_async_create_connection": "_create_connection",
    "pymongo.asynchronous.srv_resolver._SrvResolver.get_hosts": "pymongo.synchronous.srv_resolver._SrvResolver.get_hosts",
    "dns.asyncresolver.resolve": "dns.resolver.resolve",
    "__aenter__": "__enter__",
    "__aexit__": "__exit__",
}

docstring_replacements: dict[tuple[str, str], str] = {
    ("MongoClient", "connect"): """If ``True`` (the default), immediately
            begin connecting to MongoDB in the background. Otherwise connect
            on the first operation.  The default value is ``False`` when
            running in a Function-as-a-service environment.""",
    ("Collection", "create"): """If ``True``, force collection
            creation even without options being set.""",
    ("Collection", "session"): """A
            :class:`~pymongo.client_session.ClientSession` that is used with
            the create collection command.""",
    ("Collection", "kwargs"): """Additional keyword arguments will
            be passed as options for the create collection command.""",
}

docstring_removals: set[str] = {
    ".. warning:: This API is currently in beta, meaning the classes, methods, and behaviors described within may change before the full release."
}

import_replacements = {"test.synchronous": "test"}

_pymongo_base = "./pymongo/asynchronous/"
_gridfs_base = "./gridfs/asynchronous/"
_test_base = "./test/asynchronous/"

_pymongo_dest_base = "./pymongo/synchronous/"
_gridfs_dest_base = "./gridfs/synchronous/"
_test_dest_base = "./test/"

if not Path.exists(Path(_pymongo_dest_base)):
    Path.mkdir(Path(_pymongo_dest_base))
if not Path.exists(Path(_gridfs_dest_base)):
    Path.mkdir(Path(_gridfs_dest_base))

async_files = [_pymongo_base + f.name for f in Path(_pymongo_base).iterdir() if f.is_file()]

gridfs_files = [_gridfs_base + f.name for f in Path(_gridfs_base).iterdir() if f.is_file()]


def async_only_test(f: Path) -> bool:
    """Return True for async tests that should not be converted to sync."""
    return f.name in [
        "test_locks.py",
        "test_concurrency.py",
        "test_async_cancellation.py",
        "test_async_loop_safety.py",
        "test_async_contextvars_reset.py",
        "test_async_loop_unblocked.py",
        "test_async_network_layer.py",
    ]


test_files = [
    _test_base + f.name
    for f in Path(_test_base).iterdir()
    if f.is_file() and not async_only_test(f)
]


def process_files(
    files: list[str], docstring_translate_files: list[str], sync_test_files: list[str]
) -> None:
    for file in files:
        if "__init__" not in file or ("__init__" and "test" in file):
            with open(file, "r+") as f:
                lines = f.readlines()
                lines = apply_is_sync(lines, file)
                lines = translate_coroutine_types(lines)
                lines = translate_async_sleeps(lines)
                if file in docstring_translate_files:
                    lines = translate_docstrings(lines)
                if file in sync_test_files:
                    lines = translate_imports(lines)
                lines = process_ignores(lines)
                f.seek(0)
                f.writelines(lines)
                f.truncate()


def apply_is_sync(lines: list[str], file: str) -> list[str]:
    try:
        is_sync = next(iter([line for line in lines if line.startswith("_IS_SYNC = ")]))
        index = lines.index(is_sync)
        is_sync = is_sync.replace("False", "True")
        lines[index] = is_sync
    except StopIteration:
        print(
            f"Missing _IS_SYNC at top of async file {file.replace('synchronous', 'asynchronous')}"
        )
        raise
    return lines


def translate_coroutine_types(lines: list[str]) -> list[str]:
    coroutine_types = [line for line in lines if "Coroutine[" in line]
    for type in coroutine_types:
        res = re.search(r"Coroutine\[([A-z]+), ([A-z]+), ([A-z]+)\]", type)
        if res:
            old = res[0]
            index = lines.index(type)
            new = type.replace(old, res.group(3))
            lines[index] = new
    coroutine_types = [line for line in lines if "Awaitable[" in line]
    for type in coroutine_types:
        res = re.search(r"Awaitable\[([A-z]+)\]", type)
        if res:
            old = res[0]
            index = lines.index(type)
            new = type.replace(old, res.group(1))
            lines[index] = new
    return lines


def translate_imports(lines: list[str]) -> list[str]:
    for k, v in import_replacements.items():
        matches = [line for line in lines if k in line and "import" in line]
        for line in matches:
            index = lines.index(line)
            lines[index] = line.replace(k, v)
    return lines


def translate_async_sleeps(lines: list[str]) -> list[str]:
    blocking_sleeps = [line for line in lines if "asyncio.sleep(0)" in line]
    lines = [line for line in lines if line not in blocking_sleeps]
    sleeps = [line for line in lines if "asyncio.sleep" in line]

    for line in sleeps:
        res = re.search(r"asyncio\.sleep\(\s*(.*?)\)", line)
        if res:
            old = res[0]
            index = lines.index(line)
            new = f"time.sleep({res[1]})"
            lines[index] = line.replace(old, new)

    return lines


def translate_docstrings(lines: list[str]) -> list[str]:
    for i in range(len(lines)):
        for k, value in replacements.items():
            if k in lines[i]:
                # This sequence of replacements fixes the grammar issues caused by translating async -> sync
                if "an Async" in lines[i]:
                    lines[i] = lines[i].replace("an Async", "a Async")
                if "an 'Async" in lines[i]:
                    lines[i] = lines[i].replace("an 'Async", "a 'Async")
                if "An Async" in lines[i]:
                    lines[i] = lines[i].replace("An Async", "A Async")
                if "An 'Async" in lines[i]:
                    lines[i] = lines[i].replace("An 'Async", "A 'Async")
                if "an asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("an asynchronous", "a")
                if "An asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("An asynchronous", "A")
                # This sentence states the callback contract, whose meaning
                # would invert under the async -> sync word replacements.
                if "Must be a coroutine function for the asynchronous API." in lines[i]:
                    lines[i] = lines[i].replace(
                        "Must be a coroutine function for the asynchronous API.",
                        "Must be a regular function.",
                    )
                # This ensures docstring links are for `pymongo.X` instead of `pymongo.synchronous.X`
                if "pymongo.asynchronous" in lines[i] and "import" not in lines[i]:
                    lines[i] = lines[i].replace("pymongo.asynchronous", "pymongo")
                lines[i] = lines[i].replace(k, value)
            if "Sync" in lines[i] and "Synchronous" not in lines[i] and value in lines[i]:
                lines[i] = lines[i].replace("Sync", "")
        if "async for" in lines[i] or "async with" in lines[i] or "async def" in lines[i]:
            if "an async " in lines[i]:
                lines[i] = lines[i].replace("an async ", "a ")
            lines[i] = lines[i].replace("async ", "")
        if "await " in lines[i] and "tailable" not in lines[i]:
            lines[i] = lines[i].replace("await ", "")
    for i in range(len(lines)):
        for k, value in docstring_replacements.items():  # type: ignore[assignment]
            if f":param {k[1]}: **Not supported by {k[0]}**." in lines[i]:
                lines[i] = lines[i].replace(
                    f"**Not supported by {k[0]}**.",
                    value,
                )

        for line in docstring_removals:
            if line in lines[i]:
                lines[i] = "DOCSTRING_REMOVED"
                lines[i + 1] = "DOCSTRING_REMOVED"

    return [line for line in lines if line != "DOCSTRING_REMOVED"]


def process_ignores(lines: list[str]) -> list[str]:
    for i in range(len(lines)):
        for k, v in replacements.items():
            if "unasync: off" in lines[i] and v in lines[i]:
                lines[i] = lines[i].replace(v, k)
    return lines


def git_tracked_files(dests: list[Path]) -> set[Path] | None:
    """Return the subset of dests git tracks, or None when git is unusable.

    A ``None`` result (no git, or outside a repository, as in the test
    tree) makes callers treat every destination as tracked, which skips
    unchanged files as before.
    """
    if not dests:
        return set()
    command = ["git", "ls-files", "--", *(str(dest) for dest in dests)]
    try:
        result = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return {Path(line) for line in result.stdout.splitlines()}


def copy_changed_files(
    pairs: list[tuple[Path, Path]],
    manifest: Path | None = None,
    tracked: set[Path] | None = None,
) -> list[Path]:
    """Copy staged files into the working tree when their content differs.

    Each destination is written through a temporary sibling that then
    atomically replaces it, so a write that fails midway (for example when
    the disk fills) leaves that destination untouched. The original is
    hard-linked aside first, so the rollback restores it by rename and
    needs no disk space. If a later step fails or is interrupted,
    destinations already written are restored, so the working tree is left
    unchanged; publishing ``manifest`` inside that same rollback scope
    extends the guarantee to manifest write failures. An unchanged
    destination that ``tracked`` does not contain is listed in the
    manifest without being rewritten, so a newly generated module reaches
    the staging hook on its first commit.
    """
    written: list[tuple[Path, bytes | None, Path | None]] = []
    replaced: list[Path] = []
    included: list[Path] = []
    try:
        for staged, dest in pairs:
            new = staged.read_bytes()
            old = dest.read_bytes() if dest.is_file() else None
            if old == new:
                if tracked is not None and dest not in tracked:
                    included.append(dest)
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            backup: Path | None = None
            if old is not None:
                backup = dest.with_name(dest.name + ".synchro-backup")
                try:
                    backup.unlink(missing_ok=True)
                    os.link(dest, backup)
                except OSError:
                    # No hard-link support: fall back to the in-memory restore.
                    backup = None
            # Track before replacing so an interrupt cannot skip the rollback.
            written.append((dest, old, backup))
            tmp = dest.with_name(dest.name + ".synchro-tmp")
            try:
                tmp.write_bytes(new)
                tmp.replace(dest)
            except BaseException:
                try:
                    landed = dest.read_bytes() == new
                except OSError:
                    landed = True  # cannot verify; keep the entry and attempt the restore
                if not landed:
                    # The rename is atomic: nothing was published, so the
                    # destination still holds its original and the rollback
                    # must not rewrite it.
                    written.pop()
                    if backup is not None:
                        backup.unlink(missing_ok=True)
                raise
            finally:
                tmp.unlink(missing_ok=True)
            replaced.append(dest)
            included.append(dest)
        if manifest is not None:
            names = [f"./{dest.as_posix()}" for dest in included]
            tmp_manifest = manifest.with_name(manifest.name + ".synchro-tmp")
            try:
                tmp_manifest.write_text("\n".join(names) + ("\n" if names else ""))
                tmp_manifest.replace(manifest)
            finally:
                tmp_manifest.unlink(missing_ok=True)
    except BaseException:
        # Restore in reverse so each restore releases space for the next.
        # Any exception, including Ctrl-C, must restore the working tree,
        # and a failing restore must not strand the other destinations.
        for dest, old, backup in reversed(written):
            try:
                if backup is not None:
                    backup.replace(dest)
                elif old is None:
                    dest.unlink(missing_ok=True)
                else:
                    dest.write_bytes(old)
            except BaseException as restore_error:
                print(
                    f"Synchro: could not restore {dest}: {restore_error!r}",
                    file=sys.stderr,
                )
        raise
    finally:
        # Drop the backups: consumed by the rollback above, or not needed
        # because publication succeeded.
        for _, _, backup in written:
            if backup is not None:
                backup.unlink(missing_ok=True)
    return replaced


def prepare_staging() -> None:
    """Recreate the out-of-tree staging area used to generate sync files.

    A copy of pyproject.toml at the staging root keeps ruff's
    per-file-ignores (e.g. ``test/*.py``) and first-party import
    classification identical to a run against the repo root.
    """
    shutil.rmtree(STAGING, ignore_errors=True)
    for base in (_pymongo_dest_base, _gridfs_dest_base, _test_dest_base):
        Path(STAGING, base[2:]).mkdir(parents=True, exist_ok=True)
    shutil.copyfile("pyproject.toml", f"{STAGING}/pyproject.toml")
    config = Path(STAGING, "pyproject.toml")
    lines = config.read_text().splitlines(keepends=True)
    for i, line in enumerate(lines):
        if line.startswith("isort.required-imports"):
            lines.insert(i + 1, 'isort.known-first-party = ["pymongo", "gridfs", "bson", "test"]\n')
            break
    else:
        print(
            "Synchro: no isort.required-imports line in pyproject.toml, so "
            "isort.known-first-party was not added to the staged ruff config and "
            "import classification may differ from the repo root.",
            file=sys.stderr,
        )
    config.write_text("".join(lines))


def mirrored_test_files(filtered_tests: list[str]) -> list[str]:
    """Return the generated test paths for the given async test files.

    A test file is mirrored only when its synchronous counterpart already
    exists in ``test/``.
    """
    return [
        _test_dest_base + Path(f).name
        for f in filtered_tests
        if (Path(_test_dest_base) / Path(f).name).is_file()
    ]


def unasync_directory(files: list[str], src: str, dest: str, replacements: dict[str, str]) -> None:
    unasync_files(
        files,
        [
            Rule(
                fromdir=src,
                todir=dest,
                additional_replacements=replacements,
            )
        ],
    )


def main() -> None:
    is_ci = bool(os.environ.get("CI"))
    modified_files = [f"./{f}" for f in sys.argv[1:]]
    errored = False
    for fname in async_files + gridfs_files + test_files:
        # If the async file was modified, we don't need to check if the sync file was also modified.
        if str(fname) in modified_files:
            continue
        sync_name = str(fname).replace("asynchronous", "synchronous")
        test_sync_name = str(fname).replace("/asynchronous", "")
        if sync_name in modified_files or (
            test_sync_name in modified_files and "OVERRIDE_SYNCHRO_CHECK" not in os.environ
        ):
            print(f"Refusing to overwrite {test_sync_name}")
            errored = True
    if errored:
        sys.exit(1)

    # When called with specific files, only process those; otherwise process everything.
    modified_set = set(modified_files)
    filtered_async = [f for f in async_files if not modified_set or f in modified_set]
    filtered_gridfs = [f for f in gridfs_files if not modified_set or f in modified_set]
    filtered_tests = [f for f in test_files if not modified_set or f in modified_set]

    # Check async source files for problems before generating sync output.
    async_sources = filtered_async + filtered_gridfs + filtered_tests
    if async_sources:
        subprocess.run(  # noqa: S603
            [sys.executable, "-m", "ruff", "check", *async_sources],
            check=True,
        )

    try:
        prepare_staging()
        generated_pymongo = [_pymongo_dest_base + Path(f).name for f in filtered_async]
        generated_gridfs = [_gridfs_dest_base + Path(f).name for f in filtered_gridfs]
        generated_tests = mirrored_test_files(filtered_tests)
        generated_names = generated_pymongo + generated_gridfs + generated_tests
        staged_pymongo = [f"{STAGING}/{name[2:]}" for name in generated_pymongo]
        staged_gridfs = [f"{STAGING}/{name[2:]}" for name in generated_gridfs]
        staged_tests = [f"{STAGING}/{name[2:]}" for name in generated_tests]
        staged_names = staged_pymongo + staged_gridfs + staged_tests

        unasync_directory(
            filtered_async, _pymongo_base, f"{STAGING}/{_pymongo_dest_base[2:]}", replacements
        )
        unasync_directory(
            filtered_gridfs, _gridfs_base, f"{STAGING}/{_gridfs_dest_base[2:]}", replacements
        )
        unasync_directory(
            filtered_tests, _test_base, f"{STAGING}/{_test_dest_base[2:]}", replacements
        )

        process_files(staged_names, staged_names, staged_tests)

        if staged_names:
            try:
                subprocess.run(  # noqa: S603
                    [sys.executable, "-m", "ruff", "check", *staged_names, "--fix"],
                    check=True,
                )
                subprocess.run(  # noqa: S603
                    [sys.executable, "-m", "ruff", "format", *staged_names],
                    check=True,
                )
            except subprocess.CalledProcessError:
                print(
                    "Synchro: ruff failed on the generated sync output. Fix the error in "
                    "the corresponding async source file and run the synchro hook again; "
                    "the working tree was left unchanged.",
                    file=sys.stderr,
                )
                sys.exit(1)

        pairs = [
            (Path(staged), Path(name))
            for staged, name in zip(staged_names, generated_names, strict=True)
        ]
        # In CI the drift itself is the report, so neither the manifest nor
        # untracked detection runs and the copied drift is left in place.
        changed = copy_changed_files(
            pairs,
            manifest=None if is_ci else Path(MANIFEST),
            tracked=None if is_ci else git_tracked_files([dest for _, dest in pairs]),
        )
        changed_names = [f"./{dest.as_posix()}" for dest in changed]

        if is_ci and changed_names:
            print(f"Synchro generated {len(changed_names)} file(s):")
            for f in changed_names:
                print(f"  {f}")
            print("Sync files are out of date. Run `just lint --all-files synchro` to regenerate:")
            sys.exit(1)
    finally:
        shutil.rmtree(STAGING, ignore_errors=True)


if __name__ == "__main__":
    main()
