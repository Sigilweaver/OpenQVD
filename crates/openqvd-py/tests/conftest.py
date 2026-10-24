"""Shared fixtures for openqvd Python tests."""

import os
import pathlib
import pytest

# Root of the QVD-Sources downloads. Set REQUIRE_CORPUS=1 to fail instead of
# skipping when a corpus file is missing.
CORPUS_DIR = pathlib.Path(
    os.environ.get("OPENQVD_CORPUS_DIR", "/workspaces/QVD/QVD-Sources/downloads")
)
REQUIRE_CORPUS = os.environ.get("REQUIRE_CORPUS") == "1"


def _corpus_file(relative):
    p = CORPUS_DIR / relative
    if not p.exists():
        if REQUIRE_CORPUS:
            pytest.fail(f"REQUIRE_CORPUS=1 but corpus file is missing: {p}")
        pytest.skip(f"corpus not available (set OPENQVD_CORPUS_DIR): {p}")
    return str(p)


def _find_corpus_files():
    """Return a list of valid QVD files from the corpus."""
    if not CORPUS_DIR.exists():
        return []
    files = []
    for p in CORPUS_DIR.rglob("*.qvd"):
        try:
            with open(p, "rb") as f:
                head = f.read(20)
            # Skip LFS pointers and non-QVD files
            if head.startswith(b"version https://git-lfs"):
                continue
            if not (head.startswith(b"<?xml") or head.startswith(b"\xef\xbb\xbf<?xml")):
                continue
            files.append(p)
        except Exception:
            pass
    return sorted(files)


CORPUS_FILES = _find_corpus_files()


@pytest.fixture
def corpus_dir():
    """Path to the QVD corpus directory."""
    return CORPUS_DIR


@pytest.fixture
def sample_qvd():
    """Path to a small known-good QVD file (SalesReps.qvd, 4 rows x 2 cols)."""
    return _corpus_file("qlikperf/TSEEQ/Utility/QVD ShrinQer/original_qvds/SalesReps.qvd")


@pytest.fixture
def multi_type_qvd():
    """Path to tab1.qvd which has mixed types including NULLs."""
    return _corpus_file("korolmi/qvdfile/qvdfile/data/tab1.qvd")
