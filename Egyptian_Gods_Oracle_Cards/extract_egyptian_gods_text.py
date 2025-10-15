#!/usr/bin/env python3
"""Extract OCR text overlays from the Egyptian Gods oracle card scans.

The ScanSnap generated PDF already contains a hidden OCR text layer.  This
script inflates the content streams, pulls out the Tj text operators and
reconstructs the per-card labels so that we can synchronise them with the
scanned JPGs.

Usage examples
--------------
* Print the table to stdout::

    python extract_egyptian_gods_text.py

* Write a CSV table next to the PDF::

    python extract_egyptian_gods_text.py --csv Egyptian_Gods_Oracle_Cards_text.csv

* Rename the ``01.jpg`` … ``36.jpg`` scans using the extracted labels::

    python extract_egyptian_gods_text.py --rename

The script is intentionally self-contained (no third-party dependencies) so it
can run in the execution environment used for this repository.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple


@dataclass(frozen=True)
class CardLabel:
    index: int
    name: str
    keyword: str

    @property
    def index_str(self) -> str:
        return f"{self.index:02d}"


_LITERAL_RE = re.compile(rb"\((.*?)\)\s*Tj")
_HEX_LITERAL_RE = re.compile(rb"<([0-9A-Fa-f]+)>\s*Tj")
_STREAM_RE = re.compile(rb"stream\r?\n")


def _decode_tokens(stream: bytes) -> List[str]:
    tokens: List[str] = []
    raw_tokens: List[bytes] = []
    raw_tokens.extend(_LITERAL_RE.findall(stream))
    raw_tokens.extend(bytes.fromhex(match.decode()) for match in _HEX_LITERAL_RE.findall(stream))

    for token in raw_tokens:
        text = token.decode("latin-1")
        text = text.replace("\\(", "(").replace("\\)", ")").replace("\\\\", "\\")
        text = text.replace("\x7f", " ")
        text = "".join(ch for ch in text if 32 <= ord(ch) <= 126)
        text = text.strip()
        if not text:
            continue
        if any(c.islower() for c in text):
            continue
        tokens.append(text)
    return tokens


def _iter_text_streams(pdf_bytes: bytes) -> Iterator[bytes]:
    for match in _STREAM_RE.finditer(pdf_bytes):
        start = match.end()
        end = pdf_bytes.find(b"endstream", start)
        if end == -1:
            continue
        stream = pdf_bytes[start:end]
        if stream.startswith(b"\r"):
            stream = stream[1:]
        if stream.startswith(b"\n"):
            stream = stream[1:]
        try:
            inflated = zlib.decompress(stream)
        except zlib.error:
            # Non-flate (e.g. image) streams are ignored.
            continue
        if b"BT" not in inflated:
            continue
        yield inflated


def extract_labels(pdf_path: Path) -> List[CardLabel]:
    pdf_bytes = pdf_path.read_bytes()
    labels: List[CardLabel] = []
    for stream in _iter_text_streams(pdf_bytes):
        tokens = _decode_tokens(stream)
        if len(tokens) < 3:
            continue
        try:
            index = int(tokens[0])
        except ValueError:
            continue
        rest = tokens[1:]
        if len(rest) >= 2 and rest[0] == "PT" and rest[1] == "AH":
            name = "PTAH"
            keyword_tokens = rest[2:]
        else:
            name = rest[0]
            keyword_tokens = rest[1:]
        keyword = " ".join(keyword_tokens).strip()
        if not keyword:
            continue
        labels.append(CardLabel(index=index, name=name, keyword=keyword))
    labels.sort(key=lambda label: label.index)
    return labels


def _write_csv(path: Path, labels: Sequence[CardLabel]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["index", "name", "keyword"])
        for label in labels:
            writer.writerow([label.index_str, label.name, label.keyword])


def _rename_images(base_dir: Path, labels: Iterable[CardLabel]) -> None:
    for label in labels:
        source = base_dir / f"{label.index_str}.jpg"
        if not source.exists():
            print(f"[WARN] missing source {source}", file=sys.stderr)
            continue
        safe_name = label.name.replace("/", "-")
        safe_keyword = label.keyword.replace("/", "-")
        target = base_dir / f"{label.index_str} - {safe_name} - {safe_keyword}.jpg"
        if target.exists():
            print(f"[INFO] target already exists {target}", file=sys.stderr)
            continue
        source.rename(target)
        print(f"renamed {source.name} -> {target.name}")


def main(argv: Sequence[str] | None = None) -> int:
    script_dir = Path(__file__).resolve().parent
    default_pdf = script_dir / "original scanns" / "Egyptian_Gods_Oracle_Cards.pdf"

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "pdf",
        nargs="?",
        type=Path,
        default=default_pdf,
        help=f"Path to the OCR PDF (default: {default_pdf})",
    )
    parser.add_argument("--csv", type=Path, help="Write the extracted labels to a CSV file")
    parser.add_argument(
        "--rename",
        action="store_true",
        help="Rename the numbered JPG scans in the same directory as the PDF",
    )
    args = parser.parse_args(argv)

    pdf_path = args.pdf
    if not pdf_path.exists():
        parser.error(f"PDF not found: {pdf_path}")

    labels = extract_labels(pdf_path)
    if not labels:
        parser.error("no text labels found inside the PDF")

    for label in labels:
        print(f"{label.index_str}\t{label.name}\t{label.keyword}")

    if args.csv:
        _write_csv(args.csv, labels)
        print(f"Written CSV to {args.csv}")

    if args.rename:
        base_dir = pdf_path.parent.parent if pdf_path.parent.name == "original scanns" else pdf_path.parent
        _rename_images(base_dir, labels)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
