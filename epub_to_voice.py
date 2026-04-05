#!/usr/bin/env python3
"""
epub_to_voice.py – EPUB / PDF / TXT to Audio Converter
Uses edge-tts (Microsoft Edge TTS, free, no API key needed) for high-quality neural voices.

Usage:
    python epub_to_voice.py book.epub
    python epub_to_voice.py book.pdf -o ./output -v de-DE-KatjaNeural -r "+10%"
    python epub_to_voice.py book.txt
    python epub_to_voice.py --list-voices | grep de-DE
"""

import asyncio
import argparse
import re
import sys
from pathlib import Path

import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import edge_tts


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_VOICE = "de-DE-ConradNeural"  # Natural German male voice
DEFAULT_RATE  = "-10%"                # Speech speed (e.g. "+15%" for faster)
DEFAULT_VOLUME = "+0%"                # Volume adjustment


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

# Tags whose content we want to keep
KEEP_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "blockquote"}
# Tags to skip entirely (navigation, scripts, styles)
SKIP_TAGS = {"nav", "script", "style", "head"}


def sanitize_text(text: str) -> str:
    """Remove characters that confuse TTS engines (replacement chars, control chars)."""
    # Drop Unicode replacement character and other non-printable control chars
    # but keep normal whitespace (space, tab, newline)
    text = text.replace("\ufffd", "")                          # U+FFFD replacement char
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)  # control chars
    text = re.sub(r"\s+", " ", text).strip()
    return text


def html_to_text(html_bytes: bytes) -> str:
    """Extract clean readable text from an EPUB HTML chunk."""
    soup = BeautifulSoup(html_bytes, "html.parser")

    # Remove unwanted tags completely
    for tag in soup(SKIP_TAGS):
        tag.decompose()

    parts = []
    for element in soup.find_all(KEEP_TAGS):
        text = element.get_text(" ", strip=True)
        if text:
            parts.append(text)

    # Fallback: plain get_text if no recognised tags found
    if not parts:
        text = soup.get_text(" ", strip=True)
        parts = [text] if text else []

    raw = " ".join(parts)
    return sanitize_text(raw)


def chunk_text(text: str, max_chars: int = 4900) -> list[str]:
    """
    Split text into chunks ≤ max_chars without cutting in the middle of a sentence.
    edge-tts has an undocumented limit around 5 000 characters per request.
    """
    if len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    # Split on sentence-ending punctuation to find good break points
    sentences = re.split(r"(?<=[.!?])\s+", text)
    current = ""

    for sentence in sentences:
        if len(current) + len(sentence) + 1 <= max_chars:
            current = (current + " " + sentence).strip()
        else:
            if current:
                chunks.append(current)
            # If a single sentence is longer than max_chars, hard-split it
            while len(sentence) > max_chars:
                chunks.append(sentence[:max_chars])
                sentence = sentence[max_chars:]
            current = sentence

    if current:
        chunks.append(current)

    return chunks


# ---------------------------------------------------------------------------
# Core converter
# ---------------------------------------------------------------------------

class EpubConverter:
    def __init__(
        self,
        epub_path: str | Path,
        output_dir: str | Path | None = None,
        voice: str = DEFAULT_VOICE,
        rate: str = DEFAULT_RATE,
        volume: str = DEFAULT_VOLUME,
        skip_short: int = 60,
        max_chapters: int | None = None,
        merge: bool = False,
    ):
        self.epub_path    = Path(epub_path)
        self.output_dir   = (
            Path(output_dir)
            if output_dir
            else self.epub_path.parent / self.epub_path.stem
        )
        self.voice        = voice
        self.rate         = rate
        self.volume       = volume
        self.skip_short   = skip_short      # chapters with fewer chars are skipped
        self.max_chapters = max_chapters    # None = all
        self.merge        = merge           # concatenate all splits into one MP3

    # ------------------------------------------------------------------
    @staticmethod
    def _clean_title(raw: str) -> str:
        """Strip author prefix/suffix from a raw title string."""
        for sep in (" - ", " – ", ": "):
            if sep in raw:
                raw = raw.split(sep, 1)[-1].strip()
                break
        raw = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
        return raw

    # ------------------------------------------------------------------
    def _load_chapters(self) -> tuple[list[dict], str]:
        """Dispatch to the right loader based on file extension."""
        ext = self.epub_path.suffix.lower()
        if ext == ".epub":
            return self._load_epub()
        if ext == ".pdf":
            return self._load_pdf()
        if ext == ".txt":
            return self._load_txt()
        raise ValueError(f"Unsupported format: {ext}")

    # ------------------------------------------------------------------
    @staticmethod
    def _patch_ebooklib() -> None:
        """Monkey-patch ebooklib to tolerate EPUBs with broken NCX files."""
        import ebooklib.epub as _em
        _orig = _em.EpubReader._parse_ncx
        def _safe_parse_ncx(self, file_name):
            try:
                _orig(self, file_name)
            except (AttributeError, TypeError):
                pass
        _em.EpubReader._parse_ncx = _safe_parse_ncx

    # ------------------------------------------------------------------
    def _load_epub(self) -> tuple[list[dict], str]:
        """Read an EPUB and return (chapters, clean title)."""
        self._patch_ebooklib()
        book  = epub.read_epub(str(self.epub_path), options={"ignore_ncx": True})
        meta  = book.get_metadata("DC", "title")
        raw   = meta[0][0].strip() if meta else self.epub_path.stem
        title = self._clean_title(raw) or self.epub_path.stem

        items    = {item.get_id(): item for item in book.get_items()}
        chapters: list[dict] = []

        for spine_id, _ in book.spine:
            item = items.get(spine_id)
            if item is None or item.get_type() != ebooklib.ITEM_DOCUMENT:
                continue
            text = html_to_text(item.get_content())
            if len(text) < self.skip_short:
                continue
            soup  = BeautifulSoup(item.get_content(), "html.parser")
            h_tag = soup.find(["h1", "h2", "h3"])
            chapter_title = h_tag.get_text(strip=True) if h_tag else item.get_name()
            chapters.append({"title": chapter_title, "text": text})

        return chapters, title

    # ------------------------------------------------------------------
    def _load_pdf(self) -> tuple[list[dict], str]:
        """Read a PDF and return (chapters grouped by pages, clean title)."""
        import fitz  # pymupdf

        doc   = fitz.open(str(self.epub_path))
        raw   = (doc.metadata.get("title") or "").strip() or self.epub_path.stem
        title = self._clean_title(raw) or self.epub_path.stem

        # Accumulate page text; start a new chapter when accumulated ≥ TARGET chars
        TARGET    = 15_000
        chapters: list[dict] = []
        buf       = ""
        page_start = 1

        for i, page in enumerate(doc, start=1):
            buf += page.get_text()
            at_end = (i == len(doc))
            if len(buf) >= TARGET or at_end:
                text = sanitize_text(buf)
                if len(text) >= self.skip_short:
                    label = f"Seiten {page_start}–{i}" if page_start < i else f"Seite {i}"
                    chapters.append({"title": label, "text": text})
                buf        = ""
                page_start = i + 1

        doc.close()
        return chapters, title

    # ------------------------------------------------------------------
    def _load_txt(self) -> tuple[list[dict], str]:
        """Read a plain-text file and return (sections as chapters, clean title)."""
        raw_title = self._clean_title(self.epub_path.stem) or self.epub_path.stem

        content = self.epub_path.read_text(encoding="utf-8", errors="replace")

        # Split on lines that look like chapter headings or on multiple blank lines
        HEADING = re.compile(
            r"(?m)^(?:Kapitel|Chapter|Teil|Part|Abschnitt|Section)\s+\S.*$"
        )
        # Mark split points then split
        marked  = HEADING.sub("\x00\\g<0>", content)
        parts   = re.split(r"\x00|\n{3,}", marked)

        chapters: list[dict] = []
        for i, part in enumerate(parts, start=1):
            text = sanitize_text(part)
            if len(text) >= self.skip_short:
                # Use first non-empty line as title if it's short enough
                first_line = part.strip().splitlines()[0][:80] if part.strip() else f"Abschnitt {i}"
                chapters.append({"title": first_line, "text": text})

        # Fallback: no sections found → split into equal chunks
        if not chapters:
            full = sanitize_text(content)
            CHUNK = 20_000
            for i, start in enumerate(range(0, len(full), CHUNK), start=1):
                text = full[start:start + CHUNK]
                if len(text) >= self.skip_short:
                    chapters.append({"title": f"Abschnitt {i}", "text": text})

        return chapters, raw_title

    # ------------------------------------------------------------------
    async def _synthesise_chunk(self, text: str, out_path: Path) -> None:
        """Synthesise a single text chunk (≤ max_chars) to one MP3 file."""
        communicate = edge_tts.Communicate(text, self.voice, rate=self.rate, volume=self.volume)
        await communicate.save(str(out_path))
        # Sanity-check: an MP3 with audio is always > 1 KB
        if out_path.exists() and out_path.stat().st_size < 1024:
            out_path.unlink()
            raise RuntimeError(f"Edge-TTS produced an empty/corrupt file – text may contain unsupported characters.")

    # ------------------------------------------------------------------
    async def convert(self) -> None:
        """Main entry point – convert all chapters to MP3 files."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        chapters, book_title = self._load_chapters()
        if self.max_chapters:
            chapters = chapters[:self.max_chapters]

        if not chapters:
            print("⚠  No readable chapters found – is this a valid EPUB?", file=sys.stderr)
            sys.exit(1)

        safe_book = re.sub(r'[\\/:*?"<>|]', "_", book_title)[:80]

        # Pre-compute all (chapter_idx, split_idx, chunk) tuples so we know the total
        jobs: list[tuple[int, int, str]] = []
        for ch_idx, chapter in enumerate(chapters, start=1):
            for sp_idx, chunk in enumerate(chunk_text(chapter["text"]), start=1):
                jobs.append((ch_idx, sp_idx, chunk))

        total = len(jobs)
        print(f"📖  Book:     {book_title}")
        print(f"🔊  Voice:    {self.voice}  |  Rate: {self.rate}")
        print(f"📂  Output:   {self.output_dir}")
        print(f"📑  Chapters: {len(chapters)}  |  Splits: {total}\n")

        produced: list[Path] = []

        for counter, (ch_idx, sp_idx, chunk) in enumerate(jobs, start=1):
            filename = self.output_dir / f"{ch_idx:03d}_{sp_idx:03d}_{safe_book}.mp3"
            chars    = len(chunk)

            print(f"  [{counter:>3}/{total}]  {ch_idx:03d}_{sp_idx:03d}  ({chars:,} chars) … ", end="", flush=True)

            try:
                await self._synthesise_chunk(chunk, filename)
                produced.append(filename)
                print("✓")
            except Exception as exc:
                print(f"✗  ERROR: {exc}")

        if self.merge and produced:
            merged = self.output_dir / f"{safe_book}.mp3"
            print(f"\n🔗  Merging {len(produced)} file(s) → {merged.name} … ", end="", flush=True)
            with open(merged, "wb") as fout:
                for part in produced:
                    fout.write(part.read_bytes())
                    part.unlink()
            print("✓")
            print(f"\n✅  Done! Files saved to: {self.output_dir}")
        else:
            print(f"\n✅  Done! Files saved to: {self.output_dir}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def list_voices_cmd(filter_str: str = "") -> None:
    voices = await edge_tts.list_voices()
    filtered = [v for v in voices if filter_str.lower() in v["ShortName"].lower()] if filter_str else voices
    print(f"{'Short Name':<35}  {'Locale':<10}  Gender")
    print("-" * 60)
    for v in filtered:
        print(f"{v['ShortName']:<35}  {v['Locale']:<10}  {v['Gender']}")
    print(f"\n{len(filtered)} voice(s) found.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lightweight EPUB → MP3 converter powered by edge-tts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python epub_to_voice.py book.epub
  python epub_to_voice.py book.epub -o D:/Audiobooks/MyBook -v en-US-AriaNeural -r "+15%"
  python epub_to_voice.py --list-voices de
        """,
    )
    parser.add_argument("epub", nargs="?", help="Path to the .epub file")
    parser.add_argument("-o", "--output",  help="Output directory (default: <epub_stem>/)")
    parser.add_argument("-v", "--voice",   default=DEFAULT_VOICE, help=f"TTS voice (default: {DEFAULT_VOICE})")
    parser.add_argument("-r", "--rate",    default=DEFAULT_RATE,  help='Speech rate, e.g. "+15%%" or "-10%%"')
    parser.add_argument("--volume",        default=DEFAULT_VOLUME, help='Volume, e.g. "+10%%"')
    parser.add_argument("--skip-short",    type=int, default=60,
                        help="Skip chapters with fewer than N characters (default: 60)")
    parser.add_argument("--max-chapters",  type=int, default=None,
                        help="Convert only the first N chapters (e.g. 3 for a preview)")
    parser.add_argument("--merge", action="store_true",
                        help="Concatenate all splits into a single MP3 file")
    parser.add_argument("--list-voices",  nargs="?", const="", metavar="FILTER",
                        help='List available voices, optionally filtered (e.g. --list-voices de)')
    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    if args.list_voices is not None:
        asyncio.run(list_voices_cmd(args.list_voices))
        return

    if not args.epub:
        parser.print_help()
        sys.exit(1)

    SUPPORTED = {".epub", ".pdf", ".txt"}
    epub_path = Path(args.epub)
    if not epub_path.exists():
        print(f"Error: File not found: {epub_path}", file=sys.stderr)
        sys.exit(1)
    if epub_path.suffix.lower() not in SUPPORTED:
        print(f"Error: Unsupported format '{epub_path.suffix}'. Supported: {', '.join(SUPPORTED)}", file=sys.stderr)
        sys.exit(1)

    converter = EpubConverter(
        epub_path    = epub_path,
        output_dir   = args.output,
        voice        = args.voice,
        rate         = args.rate,
        volume       = args.volume,
        skip_short   = args.skip_short,
        max_chapters = args.max_chapters,
        merge        = args.merge,
    )
    asyncio.run(converter.convert())


if __name__ == "__main__":
    main()
