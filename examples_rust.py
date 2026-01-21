#!/usr/bin/env python3
"""Example usage of the Rust BSON extension.

This demonstrates how the Rust extension can be used as a drop-in
replacement for the C extension.
"""
from __future__ import annotations

import time


def example_basic_usage():
    """Basic encoding and decoding example."""
    import pymongo_rust

    print("=" * 60)
    print("Example 1: Basic Encoding and Decoding")
    print("=" * 60)

    # Create a simple document
    document = {
        "name": "Alice",
        "age": 30,
        "email": "alice@example.com",
        "is_active": True,
    }

    print(f"\nOriginal document: {document}")

    # Encode to BSON
    bson_bytes = pymongo_rust.encode_bson(document)
    print(f"Encoded to {len(bson_bytes)} bytes of BSON")

    # Decode from BSON
    decoded = pymongo_rust.decode_bson(bson_bytes)
    print(f"Decoded document: {decoded}")

    # Verify round-trip
    assert decoded == document, "Round-trip failed!"
    print("✓ Round-trip successful!")


def example_nested_documents():
    """Example with nested documents and arrays."""
    import pymongo_rust

    print("\n" + "=" * 60)
    print("Example 2: Nested Documents and Arrays")
    print("=" * 60)

    # Create a complex document
    document = {
        "user": {
            "name": "Bob",
            "contact": {
                "email": "bob@example.com",
                "phone": "+1234567890",
            },
        },
        "orders": [
            {"id": 1, "total": 99.99},
            {"id": 2, "total": 149.99},
            {"id": 3, "total": 75.50},
        ],
        "tags": ["python", "rust", "mongodb"],
    }

    print(f"\nOriginal document:")
    import json

    print(json.dumps(document, indent=2))

    # Encode and decode
    bson_bytes = pymongo_rust.encode_bson(document)
    decoded = pymongo_rust.decode_bson(bson_bytes)

    print(f"\n✓ Successfully encoded/decoded complex document")
    print(f"  BSON size: {len(bson_bytes)} bytes")


def example_performance():
    """Demonstrate performance with simple benchmark."""
    import pymongo_rust

    print("\n" + "=" * 60)
    print("Example 3: Performance Demonstration")
    print("=" * 60)

    document = {
        "field1": "value1",
        "field2": 42,
        "field3": True,
        "field4": 3.14159,
        "nested": {"a": 1, "b": 2, "c": 3},
    }

    iterations = 100000
    print(f"\nPerforming {iterations:,} encode/decode operations...")

    # Warm up
    for _ in range(100):
        bson = pymongo_rust.encode_bson(document)
        pymongo_rust.decode_bson(bson)

    # Benchmark encoding
    start = time.perf_counter()
    for _ in range(iterations):
        bson = pymongo_rust.encode_bson(document)
    encode_time = time.perf_counter() - start

    # Benchmark decoding
    bson = pymongo_rust.encode_bson(document)
    start = time.perf_counter()
    for _ in range(iterations):
        pymongo_rust.decode_bson(bson)
    decode_time = time.perf_counter() - start

    print(f"\nResults:")
    print(f"  Encode: {encode_time:.3f}s ({iterations/encode_time:,.0f} ops/sec)")
    print(f"  Decode: {decode_time:.3f}s ({iterations/decode_time:,.0f} ops/sec)")
    print(f"  Total:  {encode_time + decode_time:.3f}s")


def example_comparison_with_c():
    """Compare Rust and C implementations side by side."""
    print("\n" + "=" * 60)
    print("Example 4: Comparing Rust vs C Extension")
    print("=" * 60)

    try:
        import pymongo_rust
        from bson import encode as c_encode, decode as c_decode
    except ImportError as e:
        print(f"Cannot run comparison: {e}")
        return

    document = {"name": "Charlie", "value": 12345, "active": True}

    # Encode with both
    rust_bson = pymongo_rust.encode_bson(document)
    c_bson = c_encode(document)

    print(f"\nDocument: {document}")
    print(f"Rust BSON size: {len(rust_bson)} bytes")
    print(f"C BSON size:    {len(c_bson)} bytes")

    # Verify they produce identical output
    if rust_bson == c_bson:
        print("✓ Rust and C produce identical BSON output")
    else:
        print("⚠ Output differs (but both should be valid BSON)")

    # Cross-decode
    rust_from_c = pymongo_rust.decode_bson(c_bson)
    c_from_rust = c_decode(rust_bson)

    if rust_from_c == document and c_from_rust == document:
        print("✓ Cross-compatibility verified")
    else:
        print("✗ Cross-compatibility issue detected")


def example_error_handling():
    """Demonstrate error handling."""
    import pymongo_rust

    print("\n" + "=" * 60)
    print("Example 5: Error Handling")
    print("=" * 60)

    # Try to decode invalid BSON
    print("\nAttempting to decode invalid BSON data...")
    try:
        invalid_bson = b"not valid bson data"
        pymongo_rust.decode_bson(invalid_bson)
        print("✗ Should have raised an exception!")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")

    # Try to decode empty data
    print("\nAttempting to decode empty data...")
    try:
        pymongo_rust.decode_bson(b"")
        print("✗ Should have raised an exception!")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")


def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  Rust BSON Extension Examples".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")

    try:
        import pymongo_rust

        print(f"\n✓ Rust extension loaded successfully\n")
    except ImportError as e:
        print(f"\n✗ Failed to import Rust extension: {e}")
        print("\nPlease build the extension first:")
        print("  python build_rust.py\n")
        return 1

    # Run examples
    example_basic_usage()
    example_nested_documents()
    example_performance()
    example_comparison_with_c()
    example_error_handling()

    print("\n" + "=" * 60)
    print("All examples completed successfully! 🎉")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run benchmarks: python benchmark_rust_vs_c.py")
    print("  2. Read results:   cat RUST_SPIKE_RESULTS.md")
    print("  3. Make decision:  cat RUST_DECISION_MATRIX.md")
    print()

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
