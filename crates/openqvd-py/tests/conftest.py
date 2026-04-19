"""Shared fixtures for openqvd Python tests."""

import pathlib
import pytest

CORPUS_DIR = pathlib.Path("/workspaces/QVD/QVD-Sources/downloads")


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
    p = CORPUS_DIR / "qlikperf/TSEEQ/Utility/QVD ShrinQer/original_qvds/SalesReps.qvd"
    if not p.exists():
        pytest.skip("corpus not available")
    return str(p)


@pytest.fixture
def multi_type_qvd():
    """Path to tab1.qvd which has mixed types including NULLs."""
    p = CORPUS_DIR / "korolmi/qvdfile/qvdfile/data/tab1.qvd"
    if not p.exists():
        pytest.skip("corpus not available")
    return str(p)
