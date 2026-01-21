#!/usr/bin/env python3
"""Test Binary subclasses and codec_options support in Rust extension."""
from __future__ import annotations

import sys
from bson import encode, decode
from bson.binary import Binary, USER_DEFINED_SUBTYPE, MD5_SUBTYPE, UUID_SUBTYPE
from bson.codec_options import CodecOptions


def test_binary_subtypes():
    """Test that Binary objects with various subtypes are handled correctly."""
    print("\n=== Testing Binary Subtypes ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    # Test various binary subtypes
    test_cases = [
        (0, "Generic/Default subtype"),
        (1, "Function subtype"),
        (5, "MD5 subtype"),
        (128, "User-defined subtype"),
        (200, "User-defined subtype 200"),
    ]
    
    for subtype, description in test_cases:
        try:
            # Create Binary object with subtype
            original = Binary(b"test data", subtype)
            doc = {"data": original}
            
            # Encode with Rust
            rust_encoded = pymongo_rust.encode_bson(doc)
            
            # Encode with Python for comparison
            python_encoded = encode(doc)
            
            # Verify they produce the same BSON
            if rust_encoded != python_encoded:
                print(f"✗ {description}: Encoding mismatch")
                print(f"  Rust:   {rust_encoded.hex()}")
                print(f"  Python: {python_encoded.hex()}")
                return False
            
            # Decode with Rust
            rust_decoded = pymongo_rust.decode_bson(rust_encoded)
            
            # Decode with Python for comparison
            python_decoded = decode(python_encoded)
            
            # Verify decoded value
            if subtype == 0:
                # Subtype 0 should be decoded as plain bytes
                if not isinstance(rust_decoded["data"], bytes):
                    print(f"✗ {description}: Should decode to bytes, got {type(rust_decoded['data'])}")
                    return False
                if rust_decoded["data"] != b"test data":
                    print(f"✗ {description}: Data mismatch")
                    return False
            else:
                # Other subtypes should be decoded as Binary objects
                if not isinstance(rust_decoded["data"], Binary):
                    print(f"✗ {description}: Should decode to Binary, got {type(rust_decoded['data'])}")
                    return False
                if rust_decoded["data"].subtype != subtype:
                    print(f"✗ {description}: Subtype mismatch: expected {subtype}, got {rust_decoded['data'].subtype}")
                    return False
                if bytes(rust_decoded["data"]) != b"test data":
                    print(f"✗ {description}: Data mismatch")
                    return False
            
            print(f"✓ {description} (subtype {subtype})")
            
        except Exception as e:
            print(f"✗ {description}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return True


def test_binary_subclass():
    """Test that Binary subclasses are handled correctly."""
    print("\n=== Testing Binary Subclasses ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Define a Binary subclass
        class MyBinary(Binary):
            pass
        
        # Create an instance
        original = MyBinary(b"custom binary", USER_DEFINED_SUBTYPE)
        doc = {"data": original}
        
        # Encode with Rust
        rust_encoded = pymongo_rust.encode_bson(doc)
        
        # Encode with Python for comparison
        python_encoded = encode(doc)
        
        # They should produce the same BSON
        if rust_encoded != python_encoded:
            print(f"✗ Binary subclass: Encoding mismatch")
            return False
        
        # Decode with Rust
        rust_decoded = pymongo_rust.decode_bson(rust_encoded)
        
        # Verify the decoded Binary object
        if not isinstance(rust_decoded["data"], Binary):
            print(f"✗ Binary subclass: Should decode to Binary")
            return False
        
        if rust_decoded["data"].subtype != USER_DEFINED_SUBTYPE:
            print(f"✗ Binary subclass: Subtype mismatch")
            return False
        
        if bytes(rust_decoded["data"]) != b"custom binary":
            print(f"✗ Binary subclass: Data mismatch")
            return False
        
        print(f"✓ Binary subclass handled correctly")
        return True
        
    except Exception as e:
        print(f"✗ Binary subclass test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_codec_options_accepted():
    """Test that codec_options parameter is accepted (even if not fully used yet)."""
    print("\n=== Testing Codec Options Parameter ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        doc = {"name": "test", "value": 42}
        
        # Test encode with codec_options
        opts = CodecOptions()
        encoded = pymongo_rust.encode_bson(doc, codec_options=opts)
        
        # Test decode with codec_options
        decoded = pymongo_rust.decode_bson(encoded, codec_options=opts)
        
        if decoded != doc:
            print(f"✗ Codec options: Round-trip failed")
            return False
        
        print(f"✓ Codec options parameter accepted")
        return True
        
    except Exception as e:
        print(f"✗ Codec options test failed: {e}")
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
        # Create a complex document with various Binary types
        doc = {
            "raw_bytes": b"plain bytes",
            "binary_generic": Binary(b"generic", 0),
            "binary_md5": Binary(b"\x00" * 16, MD5_SUBTYPE),
            "binary_user": Binary(b"user data", USER_DEFINED_SUBTYPE),
            "nested": {
                "binary": Binary(b"nested", 128)
            }
        }
        
        # Encode with Python, decode with Rust
        python_encoded = encode(doc)
        rust_decoded = pymongo_rust.decode_bson(python_encoded)
        
        # Verify raw bytes
        if rust_decoded["raw_bytes"] != b"plain bytes":
            print(f"✗ Cross-compat: raw_bytes mismatch")
            return False
        
        # Verify binary_generic (subtype 0 -> bytes)
        if rust_decoded["binary_generic"] != b"generic":
            print(f"✗ Cross-compat: binary_generic mismatch")
            return False
        
        # Verify binary_md5
        if not isinstance(rust_decoded["binary_md5"], Binary):
            print(f"✗ Cross-compat: binary_md5 should be Binary")
            return False
        if rust_decoded["binary_md5"].subtype != MD5_SUBTYPE:
            print(f"✗ Cross-compat: binary_md5 subtype mismatch")
            return False
        
        # Verify binary_user
        if not isinstance(rust_decoded["binary_user"], Binary):
            print(f"✗ Cross-compat: binary_user should be Binary")
            return False
        if rust_decoded["binary_user"].subtype != USER_DEFINED_SUBTYPE:
            print(f"✗ Cross-compat: binary_user subtype mismatch")
            return False
        
        # Encode with Rust, decode with Python
        rust_encoded = pymongo_rust.encode_bson(doc)
        python_decoded = decode(rust_encoded)
        
        # Verify the reverse direction
        if python_decoded["raw_bytes"] != b"plain bytes":
            print(f"✗ Cross-compat (reverse): raw_bytes mismatch")
            return False
        
        print(f"✓ Cross-compatibility verified")
        return True
        
    except Exception as e:
        print(f"✗ Cross-compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_check_keys():
    """Test that check_keys parameter works."""
    print("\n=== Testing check_keys Parameter ===")
    
    try:
        import pymongo_rust
    except ImportError as e:
        print(f"✗ Failed to import Rust extension: {e}")
        return False
    
    try:
        # Test that $ prefix is rejected when check_keys=True
        try:
            doc = {"$invalid": "value"}
            pymongo_rust.encode_bson(doc, check_keys=True)
            print(f"✗ check_keys: Should reject keys starting with '$'")
            return False
        except ValueError as e:
            if "must not start with '$'" not in str(e):
                print(f"✗ check_keys: Wrong error message: {e}")
                return False
            print(f"✓ check_keys rejects '$' prefix")
        
        # Test that . in key is rejected when check_keys=True
        try:
            doc = {"invalid.key": "value"}
            pymongo_rust.encode_bson(doc, check_keys=True)
            print(f"✗ check_keys: Should reject keys containing '.'")
            return False
        except ValueError as e:
            if "must not contain '.'" not in str(e):
                print(f"✗ check_keys: Wrong error message: {e}")
                return False
            print(f"✓ check_keys rejects '.' in keys")
        
        # Test that these keys are allowed when check_keys=False
        doc = {"$valid": "value", "valid.key": "value"}
        encoded = pymongo_rust.encode_bson(doc, check_keys=False)
        decoded = pymongo_rust.decode_bson(encoded)
        
        if decoded != doc:
            print(f"✗ check_keys: Should allow special keys when check_keys=False")
            return False
        
        print(f"✓ check_keys parameter works correctly")
        return True
        
    except Exception as e:
        print(f"✗ check_keys test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("Testing Binary Subclasses and Codec Options")
    print("=" * 60)
    
    all_passed = True
    
    all_passed &= test_binary_subtypes()
    all_passed &= test_binary_subclass()
    all_passed &= test_codec_options_accepted()
    all_passed &= test_cross_compatibility()
    all_passed &= test_check_keys()
    
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
