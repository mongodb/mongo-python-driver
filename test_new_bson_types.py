#!/usr/bin/env python3
"""Test script to verify the newly implemented BSON types."""
from __future__ import annotations

import sys
import datetime
from bson import encode, decode
from bson.objectid import ObjectId
from bson.datetime_ms import DatetimeMS
from bson.regex import Regex
from bson.timestamp import Timestamp
from bson.tz_util import utc


def test_objectid():
    """Test ObjectId encoding and decoding."""
    print("\n=== Testing ObjectId ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Create an ObjectId
        oid = ObjectId()
        doc = {"_id": oid, "name": "test"}
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # Verify they produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ ObjectId: Encoding mismatch")
            print(f"  Rust:   {rust_encoded.hex()}")
            print(f"  Python: {python_encoded.hex()}")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify the decoded ObjectId
        if not isinstance(rust_decoded["_id"], ObjectId):
            print(f"✗ ObjectId: Should decode to ObjectId, got {type(rust_decoded['_id'])}")
            return False
        
        if rust_decoded["_id"] != oid:
            print(f"✗ ObjectId: Value mismatch")
            print(f"  Expected: {oid}")
            print(f"  Got:      {rust_decoded['_id']}")
            return False
        
        # Test specific ObjectId from hex string
        hex_oid = ObjectId("507f1f77bcf86cd799439011")
        doc2 = {"oid": hex_oid}
        rust_encoded2 = pymongo_rust.encode_bson(doc2)
        rust_decoded2 = pymongo_rust.decode_bson(rust_encoded2)
        
        if rust_decoded2["oid"] != hex_oid:
            print(f"✗ ObjectId: Hex string ObjectId mismatch")
            return False
        
        print(f"✓ ObjectId encoding and decoding works correctly")
        return True
        
    except Exception as e:
        print(f"✗ ObjectId test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_datetime():
    """Test DateTime encoding and decoding."""
    print("\n=== Testing DateTime ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Test with Python datetime
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=utc)
        doc = {"timestamp": dt}
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # Verify they produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ DateTime: Encoding mismatch")
            print(f"  Rust:   {rust_encoded.hex()}")
            print(f"  Python: {python_encoded.hex()}")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify the decoded datetime
        if not isinstance(rust_decoded["timestamp"], datetime.datetime):
            print(f"✗ DateTime: Should decode to datetime, got {type(rust_decoded['timestamp'])}")
            return False
        
        # Compare timestamps (allow millisecond precision)
        if abs((rust_decoded["timestamp"] - dt).total_seconds()) > 0.001:
            print(f"✗ DateTime: Value mismatch")
            print(f"  Expected: {dt}")
            print(f"  Got:      {rust_decoded['timestamp']}")
            return False
        
        # Test with DatetimeMS
        dtms = DatetimeMS(1234567890123)
        doc2 = {"dtms": dtms}
        rust_encoded2 = pymongo_rust.encode_bson(doc2)
        rust_decoded2 = pymongo_rust.decode_bson(rust_encoded2)
        
        if not isinstance(rust_decoded2["dtms"], datetime.datetime):
            print(f"✗ DatetimeMS: Should decode to datetime, got {type(rust_decoded2['dtms'])}")
            return False
        
        print(f"✓ DateTime and DatetimeMS encoding and decoding works correctly")
        return True
        
    except Exception as e:
        print(f"✗ DateTime test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_regex():
    """Test Regex encoding and decoding."""
    print("\n=== Testing Regex ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Test with Regex object
        regex = Regex(r"^test.*", "i")
        doc = {"pattern": regex}
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # Verify they produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ Regex: Encoding mismatch")
            print(f"  Rust:   {rust_encoded.hex()}")
            print(f"  Python: {python_encoded.hex()}")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify the decoded regex
        if not isinstance(rust_decoded["pattern"], Regex):
            print(f"✗ Regex: Should decode to Regex, got {type(rust_decoded['pattern'])}")
            return False
        
        if rust_decoded["pattern"].pattern != regex.pattern:
            print(f"✗ Regex: Pattern mismatch")
            print(f"  Expected: {regex.pattern}")
            print(f"  Got:      {rust_decoded['pattern'].pattern}")
            return False
        
        # Test with integer flags
        import re
        regex2 = Regex(r"test", re.IGNORECASE | re.MULTILINE)
        doc2 = {"pattern": regex2}
        rust_encoded2 = pymongo_rust.encode_bson(doc2)
        rust_decoded2 = pymongo_rust.decode_bson(rust_encoded2)
        
        if not isinstance(rust_decoded2["pattern"], Regex):
            print(f"✗ Regex: Should decode to Regex with int flags")
            return False
        
        print(f"✓ Regex encoding and decoding works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Regex test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_timestamp():
    """Test Timestamp encoding and decoding."""
    print("\n=== Testing Timestamp ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Test with Timestamp object
        ts = Timestamp(1234567890, 1)
        doc = {"ts": ts}
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # Verify they produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ Timestamp: Encoding mismatch")
            print(f"  Rust:   {rust_encoded.hex()}")
            print(f"  Python: {python_encoded.hex()}")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify the decoded timestamp
        if not isinstance(rust_decoded["ts"], Timestamp):
            print(f"✗ Timestamp: Should decode to Timestamp, got {type(rust_decoded['ts'])}")
            return False
        
        if rust_decoded["ts"].time != ts.time:
            print(f"✗ Timestamp: Time mismatch")
            print(f"  Expected: {ts.time}")
            print(f"  Got:      {rust_decoded['ts'].time}")
            return False
        
        if rust_decoded["ts"].inc != ts.inc:
            print(f"✗ Timestamp: Inc mismatch")
            print(f"  Expected: {ts.inc}")
            print(f"  Got:      {rust_decoded['ts'].inc}")
            return False
        
        print(f"✓ Timestamp encoding and decoding works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Timestamp test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_mixed_types():
    """Test a document with all new types."""
    print("\n=== Testing Mixed Document ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Create a document with all types
        doc = {
            "_id": ObjectId(),
            "created": datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=utc),
            "pattern": Regex(r"^test", "i"),
            "oplog_ts": Timestamp(1234567890, 1),
            "data": {"nested": "value"},
            "tags": ["tag1", "tag2"],
        }
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # Verify they produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ Mixed document: Encoding mismatch")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify all fields
        if not isinstance(rust_decoded["_id"], ObjectId):
            print(f"✗ Mixed document: _id type mismatch")
            return False
        
        if not isinstance(rust_decoded["created"], datetime.datetime):
            print(f"✗ Mixed document: created type mismatch")
            return False
        
        if not isinstance(rust_decoded["pattern"], Regex):
            print(f"✗ Mixed document: pattern type mismatch")
            return False
        
        if not isinstance(rust_decoded["oplog_ts"], Timestamp):
            print(f"✗ Mixed document: oplog_ts type mismatch")
            return False
        
        print(f"✓ Mixed document with all types works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Mixed document test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cross_compatibility():
    """Test that Rust and Python implementations are compatible."""
    print("\n=== Testing Cross-Compatibility ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        doc = {
            "_id": ObjectId(),
            "created": datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=utc),
            "pattern": Regex(r"^test", "i"),
            "ts": Timestamp(1234567890, 1),
        }
        
        # Encode with Python, decode with Rust
        python_encoded = encode(doc)
        rust_decoded = pymongo_rust.decode_bson(python_encoded)
        
        # Verify types
        if not isinstance(rust_decoded["_id"], ObjectId):
            print(f"✗ Cross-compat: _id should be ObjectId")
            return False
        
        if not isinstance(rust_decoded["created"], datetime.datetime):
            print(f"✗ Cross-compat: created should be datetime")
            return False
        
        if not isinstance(rust_decoded["pattern"], Regex):
            print(f"✗ Cross-compat: pattern should be Regex")
            return False
        
        if not isinstance(rust_decoded["ts"], Timestamp):
            print(f"✗ Cross-compat: ts should be Timestamp")
            return False
        
        # Encode with Rust, decode with Python
        rust_encoded = pymongo_rust.encode_bson(doc)
        python_decoded = decode(rust_encoded)
        
        # Verify the reverse direction
        if not isinstance(python_decoded["_id"], ObjectId):
            print(f"✗ Cross-compat (reverse): _id should be ObjectId")
            return False
        
        print(f"✓ Cross-compatibility verified")
        return True
        
    except Exception as e:
        print(f"✗ Cross-compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("Testing New BSON Types (ObjectId, DateTime, Regex, Timestamp)")
    print("=" * 60)
    
    all_passed = True
    
    all_passed &= test_objectid()
    all_passed &= test_datetime()
    all_passed &= test_regex()
    all_passed &= test_timestamp()
    all_passed &= test_mixed_types()
    all_passed &= test_cross_compatibility()
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ All tests passed!")
        print("=" * 60)
        return 0
    else:
        print("✗ Some tests failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
