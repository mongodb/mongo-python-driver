"""
Concatenate requirements files into sbom-requirements.txt at repository root.

- Includes repo_root/requirements.txt if present
- Includes all files matching repo_root/requirements/**/*.txt
- Excludes docs.txt and test.txt in the requirements folder
- Writes output to sbom-requirements.txt (overwrites)
"""

from __future__ import annotations

from pathlib import Path

EXCLUDED_NAMES = {"docs.txt", "test.txt"}


def collect_files(root: Path) -> list[Path]:
    """Collect requirement files to include in SBOM requirements.

    Args:
        root (Path): The root directory of the repository.

    Returns:
        list[Path]: A list of Paths to requirement files to include in the SBOM.
    """
    files = []

    # requirements.txt + all requirements/**/*.txt
    for p in [root / "requirements.txt", *root.glob("requirements/**/*.txt")]:
        if p.is_file() and p.name not in EXCLUDED_NAMES:
            files.append(p)

    return files


def write_combined_req_files(root: Path, files: list[Path], outname: str) -> Path:
    """Concatenate requirement files and write to outname file.

    Args:
        root (Path): The root directory of the repository.
        files (list[Path]): A list of Paths to requirement files to include in the SBOM.
        outname (str): The name of the output file.

    Raises:
        RuntimeError: If writing to the output file fails.

    Returns:
        Path: The path to the output file.
    """
    outpath = root / outname
    try:
        with outpath.open("w", encoding="utf-8") as f:
            for p in files:
                with p.open("r", encoding="utf-8") as r:
                    content = r.read().rstrip()
                if content:
                    f.write(content + "\n")
        return outpath
    except Exception as e:
        raise RuntimeError(f"Failed to write {outpath}: {e}") from e


def main() -> None:
    root = Path(__file__).parent.parent.resolve()

    files = collect_files(root)
    if not files:
        raise FileNotFoundError(f"No requirement files found from {root}")

    outpath = write_combined_req_files(root, files, "sbom-requirements.txt")
    print(f"Wrote concatenated requirements to: {outpath}")


if __name__ == "__main__":
    main()
