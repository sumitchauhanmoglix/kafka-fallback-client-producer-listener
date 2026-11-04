#!/usr/bin/env python3
"""
Verification script to ensure kafka-fallback-client-boot2 and kafka-fallback-client-boot3
remain in complete structural and API parity.

Checks:
1. File structure parity: For every class/test/resource in boot2, an equivalent exists in boot3 (and vice versa).
2. Public method & @Bean parity: Checks that all top-level public methods and @Bean declarations match between corresponding classes.
3. SPI imports parity: Confirms auto-configuration registration files exist in both.
"""

import os
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
BOOT2_DIR = ROOT_DIR / "kafka-fallback-client-boot2"
BOOT3_DIR = ROOT_DIR / "kafka-fallback-client-boot3"


def normalize_rel_path(p: Path, module_name: str) -> str:
    """Normalizes paths by stripping module names and module-specific tokens."""
    rel = str(p)
    # Strip module prefix
    rel = rel.replace(f"kafka-fallback-client-{module_name}/", "")
    # Normalize package name (com/moglix/kafka/fallback/boot2 -> com/moglix/kafka/fallback/{version})
    rel = rel.replace(f"/fallback/{module_name}/", "/fallback/{version}/")
    # Normalize class file names (Boot2KafkaFallbackProperties -> {version}KafkaFallbackProperties)
    rel = rel.replace(f"Boot2", "{version}").replace(f"Boot3", "{version}")
    rel = rel.replace(f"boot2", "{version}").replace(f"boot3", "{version}")
    return rel


def collect_files(module_dir: Path, module_name: str):
    """Collects normalized file paths mapped to actual Paths."""
    result = {}
    scan_dirs = [module_dir / "src/main", module_dir / "src/test"]
    for s_dir in scan_dirs:
        if not s_dir.exists():
            continue
        for root, _, files in os.walk(s_dir):
            for file in files:
                full_path = Path(root) / file
                rel_from_root = full_path.relative_to(ROOT_DIR)
                normalized = normalize_rel_path(rel_from_root, module_name)
                result[normalized] = full_path
    return result


def extract_top_level_methods(java_file_path: Path):
    """Extracts top-level public and @Bean methods from a Java file using brace depth tracking."""
    content = java_file_path.read_text(encoding="utf-8")

    # Strip comments
    content = re.sub(r"//.*", "", content)
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

    # Class name extraction
    class_match = re.search(r"public\s+(?:class|interface|enum)\s+(\w+)", content)
    class_name = class_match.group(1) if class_match else ""

    methods = set()
    depth = 0
    buffer = ""

    # Simple tokenizer scanning characters to track brace depth
    for char in content:
        if char == '{':
            if depth == 1:
                # We reached the opening brace of a method/field at class level
                line = buffer.strip()
                # Check if it's a public method or @Bean declaration
                # e.g.: "public KafkaFallbackOperations kafkaFallbackOperations(...)"
                # or: "@Bean\npublic CircuitBreaker kafkaFallbackCircuitBreaker(...)"
                if "public" in line and "(" in line and ")" in line:
                    # Extract method name right before '('
                    method_match = re.search(r"public\s+[\w\<\>\[\],\s]+\s+(\w+)\s*\(", line)
                    if method_match:
                        method_name = method_match.group(1)
                        if method_name != class_name:  # Exclude constructor
                            methods.add(method_name)
            depth += 1
            buffer = ""
        elif char == '}':
            depth -= 1
            buffer = ""
        else:
            if depth <= 1:
                buffer += char

    return methods


def main():
    print("=" * 60)
    print("Running Module Parity Verification (boot2 vs boot3)")
    print("=" * 60)

    if not BOOT2_DIR.exists() or not BOOT3_DIR.exists():
        print("[ERROR] Could not find boot2 or boot3 module directories!")
        sys.exit(1)

    boot2_files = collect_files(BOOT2_DIR, "boot2")
    boot3_files = collect_files(BOOT3_DIR, "boot3")

    errors = []

    # 1. File Parity Check
    boot2_only = set(boot2_files.keys()) - set(boot3_files.keys())
    boot3_only = set(boot3_files.keys()) - set(boot2_files.keys())

    if boot2_only:
        for f in sorted(boot2_only):
            errors.append(f"[MISSING IN BOOT3] File present in boot2 but missing in boot3: {f}")

    if boot3_only:
        for f in sorted(boot3_only):
            errors.append(f"[MISSING IN BOOT2] File present in boot3 but missing in boot2: {f}")

    # 2. Method Parity Check for matching Java files
    common_files = set(boot2_files.keys()) & set(boot3_files.keys())
    for norm_path in sorted(common_files):
        if not norm_path.endswith(".java"):
            continue

        p2 = boot2_files[norm_path]
        p3 = boot3_files[norm_path]

        methods_b2 = extract_top_level_methods(p2)
        methods_b3 = extract_top_level_methods(p3)

        b2_diff = methods_b2 - methods_b3
        b3_diff = methods_b3 - methods_b2

        for m in b2_diff:
            errors.append(f"[METHOD MISMATCH] '{m}()' declared in {p2.name} but missing in {p3.name}")
        for m in b3_diff:
            errors.append(f"[METHOD MISMATCH] '{m}()' declared in {p3.name} but missing in {p2.name}")

    # 3. SPI AutoConfiguration Registration Check
    spi_boot2 = BOOT2_DIR / "src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
    spi_boot3 = BOOT3_DIR / "src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"

    if not spi_boot2.exists():
        errors.append("[SPI MISSING] AutoConfiguration.imports missing in boot2")
    if not spi_boot3.exists():
        errors.append("[SPI MISSING] AutoConfiguration.imports missing in boot3")

    if errors:
        print("\n❌ PARITY CHECK FAILED:")
        print("-" * 60)
        for err in errors:
            print(f"  • {err}")
        print("-" * 60)
        print("Fix: Ensure changes made to boot2 or boot3 are mirrored in the corresponding module.")
        sys.exit(1)
    else:
        print(f"\n✅ All {len(common_files)} files and their public methods are completely in sync between boot2 and boot3!")
        print("=" * 60)
        sys.exit(0)


if __name__ == "__main__":
    main()
