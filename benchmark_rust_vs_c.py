#!/usr/bin/env python3
"""Benchmark script comparing C and Rust implementations of BSON encoding/decoding.

This script compares the performance of:
1. The existing C extension (_cbson)
2. The new Rust extension (pymongo_rust)
3. The pure Python implementation (fallback when C is not available)

Usage:
    python benchmark_rust_vs_c.py
"""
from __future__ import annotations

import sys
import time
import statistics
from typing import Any, Callable, Dict, List

# Test data structures
SIMPLE_DOC = {
    "name": "John Doe",
    "age": 30,
    "active": True,
    "score": 95.5,
}

COMPLEX_DOC = {
    "user": {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip": "10001",
        },
    },
    "orders": [
        {"id": 1, "total": 99.99, "items": ["item1", "item2", "item3"]},
        {"id": 2, "total": 149.99, "items": ["item4", "item5"]},
    ],
    "metadata": {"created": "2024-01-01", "updated": "2024-01-15", "version": 2},
}

ITERATIONS = 10000


def benchmark_function(func: Callable, iterations: int, *args: Any) -> Dict[str, float]:
    """Run a function multiple times and return timing statistics."""
    times = []

    # Warm up
    for _ in range(10):
        func(*args)

    # Actual benchmark
    for _ in range(5):
        start = time.perf_counter()
        for _ in range(iterations):
            func(*args)
        end = time.perf_counter()
        times.append((end - start) / iterations * 1_000_000)  # Convert to microseconds

    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
    }


def benchmark_rust() -> Dict[str, Any]:
    """Benchmark the Rust implementation."""
    try:
        import pymongo_rust

        print("\n=== Rust Implementation ===")

        results = {}

        # Encode simple
        print("Benchmarking simple document encoding...")
        results["encode_simple"] = benchmark_function(
            lambda: pymongo_rust.encode_bson(SIMPLE_DOC), ITERATIONS
        )

        # Decode simple
        print("Benchmarking simple document decoding...")
        simple_encoded = pymongo_rust.encode_bson(SIMPLE_DOC)
        results["decode_simple"] = benchmark_function(
            lambda: pymongo_rust.decode_bson(simple_encoded), ITERATIONS
        )

        # Encode complex
        print("Benchmarking complex document encoding...")
        results["encode_complex"] = benchmark_function(
            lambda: pymongo_rust.encode_bson(COMPLEX_DOC), ITERATIONS
        )

        # Decode complex
        print("Benchmarking complex document decoding...")
        complex_encoded = pymongo_rust.encode_bson(COMPLEX_DOC)
        results["decode_complex"] = benchmark_function(
            lambda: pymongo_rust.decode_bson(complex_encoded), ITERATIONS
        )

        # Built-in Rust benchmarks
        print("Running built-in Rust benchmarks...")
        rust_encode_simple = pymongo_rust.benchmark_encode_simple(ITERATIONS)
        rust_decode_simple = pymongo_rust.benchmark_decode_simple(ITERATIONS)
        rust_encode_complex = pymongo_rust.benchmark_encode_complex(ITERATIONS)
        rust_decode_complex = pymongo_rust.benchmark_decode_complex(ITERATIONS)

        results["builtin_encode_simple_total"] = rust_encode_simple
        results["builtin_decode_simple_total"] = rust_decode_simple
        results["builtin_encode_complex_total"] = rust_encode_complex
        results["builtin_decode_complex_total"] = rust_decode_complex

        return results
    except ImportError as e:
        print(f"Rust implementation not available: {e}")
        return {}


def benchmark_c() -> Dict[str, Any]:
    """Benchmark the C implementation."""
    try:
        from bson import encode, decode

        print("\n=== C/Python Implementation ===")

        results = {}

        # Encode simple
        print("Benchmarking simple document encoding...")
        results["encode_simple"] = benchmark_function(lambda: encode(SIMPLE_DOC), ITERATIONS)

        # Decode simple
        print("Benchmarking simple document decoding...")
        simple_encoded = encode(SIMPLE_DOC)
        results["decode_simple"] = benchmark_function(
            lambda: decode(simple_encoded), ITERATIONS
        )

        # Encode complex
        print("Benchmarking complex document encoding...")
        results["encode_complex"] = benchmark_function(lambda: encode(COMPLEX_DOC), ITERATIONS)

        # Decode complex
        print("Benchmarking complex document decoding...")
        complex_encoded = encode(COMPLEX_DOC)
        results["decode_complex"] = benchmark_function(
            lambda: decode(complex_encoded), ITERATIONS
        )

        return results
    except Exception as e:
        print(f"C implementation benchmark failed: {e}")
        return {}


def print_comparison(rust_results: Dict[str, Any], c_results: Dict[str, Any]) -> None:
    """Print a comparison of the results."""
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS COMPARISON")
    print("=" * 80)

    if not rust_results or not c_results:
        print("Unable to compare - one or both implementations not available")
        return

    print(
        f"\n{'Operation':<30} {'C (μs)':<15} {'Rust (μs)':<15} {'Speedup':<15} {'Winner':<10}"
    )
    print("-" * 80)

    operations = ["encode_simple", "decode_simple", "encode_complex", "decode_complex"]

    for op in operations:
        if op in rust_results and op in c_results:
            c_time = c_results[op]["mean"]
            rust_time = rust_results[op]["mean"]
            speedup = c_time / rust_time
            winner = "Rust" if speedup > 1.0 else "C" if speedup < 1.0 else "Tie"

            print(
                f"{op:<30} {c_time:>12.2f}   {rust_time:>12.2f}   {speedup:>12.2f}x   {winner:<10}"
            )

    print("\n" + "=" * 80)
    print("DETAILED STATISTICS")
    print("=" * 80)

    for op in operations:
        if op in rust_results and op in c_results:
            print(f"\n{op.upper()}:")
            print(f"  C Implementation:")
            for key, val in c_results[op].items():
                print(f"    {key}: {val:.2f} μs")
            print(f"  Rust Implementation:")
            for key, val in rust_results[op].items():
                print(f"    {key}: {val:.2f} μs")


def main():
    """Run all benchmarks and print results."""
    print(f"Python version: {sys.version}")
    print(f"Running {ITERATIONS} iterations for each benchmark...")

    rust_results = benchmark_rust()
    c_results = benchmark_c()

    print_comparison(rust_results, c_results)

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if rust_results and c_results:
        operations = ["encode_simple", "decode_simple", "encode_complex", "decode_complex"]
        speedups = []

        for op in operations:
            if op in rust_results and op in c_results:
                c_time = c_results[op]["mean"]
                rust_time = rust_results[op]["mean"]
                speedup = c_time / rust_time
                speedups.append(speedup)

        if speedups:
            avg_speedup = statistics.mean(speedups)
            print(f"Average speedup: {avg_speedup:.2f}x")
            if avg_speedup > 1.1:
                print("✓ Rust implementation is faster on average")
            elif avg_speedup < 0.9:
                print("✗ C implementation is faster on average")
            else:
                print("≈ Performance is roughly equivalent")
    else:
        print("Unable to generate summary - not all implementations available")

    print("\nNote: Results may vary based on system, Python version, and data complexity.")


if __name__ == "__main__":
    main()
