#!/usr/bin/env python3
"""
OSS-Fuzz seed corpus builder for BSON fuzzing, mirroring the Go driver's
(mongodb/mongo-go-driver) internal/cmd/build-oss-fuzz-corpus/main.go.

Converts the valid cases in the vendored BSON spec corpus (test/bson_corpus/
by default) to raw BSON bytes and packages them into a ZIP, plus synthesized
boundary and large stress documents.

Usage: python build_corpus.py bson_decode_seed_corpus.zip [--data-dir DIR]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import sys
import zipfile
from pathlib import Path
from typing import Any

# Force the pure-Python implementation: an ASan-instrumented _cbson would
# abort on import.
sys.modules["bson._cbson"] = None  # type: ignore[assignment]

import bson
from bson import json_util
from bson.code import Code

DEFAULT_DATA_DIR = "test/bson_corpus"


def extjson_to_bytes(extjson: str) -> bytes:
    """Convert an Extended JSON string to bytes."""
    doc = json_util.loads(extjson)
    return bson.encode(doc)


def hex_to_bytes(hex_str: str) -> bytes:
    """Decode a hex string to bytes."""
    return bytes.fromhex(hex_str)


def add_to_corpus(zw: zipfile.ZipFile, bson_bytes: bytes) -> None:
    """Add BSON bytes to the corpus ZIP, named by their SHA-256 digest."""
    filename = hashlib.sha256(bson_bytes).hexdigest()
    if filename not in zw.namelist():
        zw.writestr(filename, bson_bytes)


def seed_extjson(
    zw: zipfile.ZipFile,
    extjson: str,
    extjson_type: str,
    description: str,
) -> None:
    """Convert an Extended JSON string to BSON bytes and add it to the corpus."""
    try:
        bson_bytes = extjson_to_bytes(extjson)
    except Exception as exc:
        print(
            f"Warning: {description!r}: failed to convert {extjson_type} "
            f"extended JSON to bytes: {exc}",
            file=sys.stderr,
        )
        return
    add_to_corpus(zw, bson_bytes)


def seed_test_cases(zw: zipfile.ZipFile, test_cases: list[dict[str, Any]]) -> None:
    """Seed the corpus from a list of valid BSON corpus test cases."""
    for tc in test_cases:
        desc = tc.get("description", "<unknown>")

        # Raw BSON bytes need no JSON round-trip. A valid case can carry
        # several distinct wire representations; seed each one.
        for field in ("canonical_bson", "degenerate_bson", "converted_bson"):
            if field in tc and tc[field] is not None:
                try:
                    add_to_corpus(zw, hex_to_bytes(tc[field]))
                except ValueError as exc:
                    print(
                        f"Warning: {desc!r}: failed to decode {field} hex: {exc}",
                        file=sys.stderr,
                    )

        # Extended JSON variants, converted through the encoder to canonical BSON.
        for field, extjson_type in (
            ("canonical_extjson", "canonical"),
            ("relaxed_extjson", "relaxed"),
            ("degenerate_extjson", "degenerate"),
            ("converted_extjson", "converted"),
        ):
            if field in tc and tc[field] is not None:
                seed_extjson(zw, tc[field], extjson_type, desc)


def seed_bson_corpus(zw: zipfile.ZipFile, data_dir: str) -> None:
    """Seed the corpus from every BSON corpus JSON file in data_dir."""
    json_files = sorted(Path(data_dir).glob("*.json"))
    if not json_files:
        print(
            f"Error: no JSON files found in {data_dir!r}. "
            "The BSON spec corpus is vendored in 'test/bson_corpus'.",
            file=sys.stderr,
        )
        sys.exit(1)

    for json_file in json_files:
        try:
            with open(json_file, encoding="utf-8") as fh:
                corpus_doc = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Warning: failed to parse {json_file}: {exc}", file=sys.stderr)
            continue

        valid_cases = corpus_doc.get("valid", [])
        seed_test_cases(zw, valid_cases)


def nest(doc: dict[str, Any], depth: int) -> dict[str, Any]:
    """Wrap doc in depth levels of single-key subdocuments."""
    for _ in range(depth):
        doc = {"n": doc}
    return doc


def seed_boundary_documents(zw: zipfile.ZipFile) -> None:
    """Seed documents at the int32 declared-length and minimum-size boundaries."""
    lies: tuple[tuple[str, bytes], ...] = (
        # Declares ~2 GB, body is 8 bytes.
        ("declared_length_int32max", struct.pack("<i", 0x7FFFFFFF) + b"\x00" * 4),
        ("declared_length_0", struct.pack("<i", 0)),
        ("declared_length_4", struct.pack("<i", 4)),
    )
    for description, bson_bytes in lies:
        add_to_corpus(zw, bson_bytes)
        print(f"  seeded boundary case: {description}")
    add_to_corpus(zw, struct.pack("<i", 5) + b"\x00")
    print("  seeded boundary case: minimum_5_byte_document")


def seed_large_document(zw: zipfile.ZipFile) -> None:
    """Seed one ~500 KB document; without it, libFuzzer's max_len default
    keeps mutation below the size where large-input paths live."""
    doc = {
        "long_string": "a" * (256 * 1024),
        "many_fields": {f"f{i:05d}": i for i in range(5_000)},
        "large_array": list(range(10_000)),
        "large_subdoc": {f"nested_{i:03d}": {"s": "x" * 32} for i in range(500)},
        "code_w_scope": Code("function() {}", {"scope_string": "y" * 32768}),
        "nested_depth_100": nest({"leaf": True}, 100),
    }
    add_to_corpus(zw, bson.encode(doc))
    print("  seeded large stress document")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an OSS-Fuzz seed corpus ZIP for Python BSON fuzzing."
    )
    parser.add_argument(
        "output",
        help="Output ZIP file path, e.g. bson_decode_seed_corpus.zip",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing BSON corpus JSON files (default: {DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    if output_path.suffix != ".zip":
        print(
            f"Error: output file must have a .zip extension, got {args.output!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zw:
        seed_bson_corpus(zw, args.data_dir)
        print("Synthesized seeds:")
        seed_boundary_documents(zw)
        seed_large_document(zw)
        entry_count = len(zw.namelist())

    print(f"Wrote {entry_count} corpus entries to {output_path}")


if __name__ == "__main__":
    main()
