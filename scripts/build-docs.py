#!/usr/bin/env python3
"""Assemble the mdBook sources in book-src/ from the repository's Markdown.

README.md becomes the book's introduction and PROGRESS.md is included as
it is; docs/ is copied with its layout. A relative link that points
outside the book (source files, CLAUDE.md, licenses) is rewritten to the
file on GitHub, so every link keeps working on the published site.

Usage: python3 scripts/build-docs.py && mdbook build
"""

import os
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "book-src"
BLOB = "https://github.com/Bunty9/logstream/blob/main/"

# (repository file, path inside the book)
PAGES = [
    ("README.md", "introduction.md"),
    ("PROGRESS.md", "PROGRESS.md"),
    ("docs/operations.md", "operations.md"),
    ("docs/publishing.md", "publishing.md"),
    ("docs/specs/2026-05-28-logstream-design.md", "specs/2026-05-28-logstream-design.md"),
    ("docs/plans/2026-09-26-phases-2-4-summary.md", "plans/2026-09-26-phases-2-4-summary.md"),
    ("docs/plans/2026-05-28-logstream-phase-1-scaffold.md", "plans/2026-05-28-logstream-phase-1-scaffold.md"),
]

SUMMARY = """# Summary

[Introduction](introduction.md)

# Guides

- [Operations runbook](operations.md)
- [Publishing to crates.io](publishing.md)
- [API reference (docs.rs)](api.md)

# Project

- [Status and benchmarks](PROGRESS.md)
- [Design spec](specs/2026-05-28-logstream-design.md)
- [Phase 1: scaffold](plans/2026-05-28-logstream-phase-1-scaffold.md)
- [Phases 2-4 summary](plans/2026-09-26-phases-2-4-summary.md)
"""

API = """# API reference

The Rust API documentation is built by docs.rs:

- [`logstream-core`](https://docs.rs/logstream-core): OTLP to row mapping,
  tenant auth, the ClickHouse batcher
- [`logstream-query`](https://docs.rs/logstream-query): LogQL parser and
  ClickHouse SQL translation

The two servers install from crates.io:

```bash
cargo install logstream-ingest logstream-query
```
"""

LINK = re.compile(r"\]\((?!https?://|mailto:|#)([^)#\s]+)(#[^)]*)?\)")


def rewrite(text: str, src: str, dest: str) -> str:
    """Point each relative link at its page in the book, or at GitHub."""
    book_path = {s: d for s, d in PAGES}

    def fix(m: re.Match) -> str:
        target, anchor = m.group(1), m.group(2) or ""
        repo_path = os.path.normpath(os.path.join(os.path.dirname(src), target))
        if repo_path in book_path:
            rel = os.path.relpath(book_path[repo_path], os.path.dirname(dest) or ".")
            return f"]({rel}{anchor})"
        return f"]({BLOB}{repo_path}{anchor})"

    return LINK.sub(fix, text)


def main() -> None:
    shutil.rmtree(OUT, ignore_errors=True)
    for src, dest in PAGES:
        out = OUT / dest
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(rewrite((ROOT / src).read_text(), src, dest))
    (OUT / "SUMMARY.md").write_text(SUMMARY)
    (OUT / "api.md").write_text(API)


if __name__ == "__main__":
    main()
