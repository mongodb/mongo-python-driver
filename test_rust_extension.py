#!/usr/bin/env python3
"""Test script to verify Rust BSON implementation works correctly."""
from __future__ import annotations

import sys


def test_rust_extension():
    """Test basic functionality of the Rust extension."""
    try:
        import pymongo_rust

        print("✓ Rust extension imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        print("\nTo build the Rust extension, run:")
        print("  python build_rust.py")
        return False

    # Test encode
    try:
        test_doc = {"name": "John", "age": 30, "active": True, "score": 95.5}
        encoded = pymongo_rust.encode_bson(test_doc)
        print(f"✓ Encode test passed (produced {len(encoded)} bytes)")
    except Exception as e:
        print(f"✗ Encode test failed: {e}")
        return False

    # Test decode
    try:
        decoded = pymongo_rust.decode_bson(encoded)
        print(f"✓ Decode test passed: {decoded}")

        # Verify round-trip
        if decoded == test_doc:
            print("✓ Round-trip test passed (decoded == original)")
        else:
            print(f"✗ Round-trip test failed: {decoded} != {test_doc}")
            return False
    except Exception as e:
        print(f"✗ Decode test failed: {e}")
        return False

    # Test complex document
    try:
        complex_doc = {
            "user": {
                "name": "Jane",
                "details": {"age": 25, "email": "jane@example.com"},
            },
            "tags": ["python", "rust", "mongodb"],
            "count": 100,
        }
        encoded_complex = pymongo_rust.encode_bson(complex_doc)
        decoded_complex = pymongo_rust.decode_bson(encoded_complex)
        print(f"✓ Complex document test passed")
    except Exception as e:
        print(f"✗ Complex document test failed: {e}")
        return False

    # Test benchmarks
    try:
        time_simple = pymongo_rust.benchmark_encode_simple(1000)
        print(f"✓ Benchmark test passed (1000 iterations in {time_simple:.4f}s)")
    except Exception as e:
        print(f"✗ Benchmark test failed: {e}")
        return False

    return True


def compare_with_python_bson():
    """Compare Rust implementation with Python BSON library."""
    try:
        from bson import encode, decode
        import pymongo_rust

        print("\n--- Comparing with Python BSON library ---")

        test_doc = {"name": "Test", "value": 42, "active": True}

        # Encode with both
        python_encoded = encode(test_doc)
        rust_encoded = pymongo_rust.encode_bson(test_doc)

        print(f"Python BSON size: {len(python_encoded)} bytes")
        print(f"Rust BSON size: {len(rust_encoded)} bytes")

        # Decode with both
        python_decoded = decode(python_encoded)
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)

        # Cross-decode
        rust_from_python = pymongo_rust.decode_bson(python_encoded)
        python_from_rust = decode(rust_encoded)

        if rust_from_python == test_doc and python_from_rust == test_doc:
            print("✓ Cross-compatibility test passed")
        else:
            print("✗ Cross-compatibility test failed")
            return False

        return True

    except ImportError as e:
        print(f"⚠ Could not compare with Python BSON: {e}")
        return True  # Not a failure, just can't compare


def main():
    """Run all tests."""
    print("=" * 60)
    print("Testing Rust BSON Extension")
    print("=" * 60)
    print(f"Python version: {sys.version}\n")

    if test_rust_extension():
        print("\n" + "=" * 60)
        print("✓ All basic tests passed!")
        print("=" * 60)

        # Try comparison if possible
        compare_with_python_bson()

        print("\nYou can now run the full benchmark:")
        print("  python benchmark_rust_vs_c.py")
        return 0
    else:
        print("\n" + "=" * 60)
        print("✗ Tests failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
