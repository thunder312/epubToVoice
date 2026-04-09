#!/usr/bin/env python3
"""
epub_to_voice.py – EPUB / PDF / TXT to Audio Converter
Uses edge-tts (Microsoft Edge TTS, free, no API key needed) for high-quality neural voices.

Structure detection (headings, paragraphs) uses SSML <break> tags (Option 1).
Where SSML is not possible, spoken placeholder sentences are used (Option 3) with a warning.
Table of contents sections are automatically detected and skipped.

Usage:
    python epub_to_voice.py book.epub
    python epub_to_voice.py book.pdf -o ./output -v de-DE-KatjaNeural -r "+10%"
    python epub_to_voice.py book.txt
    python epub_to_voice.py --list-voices | grep de-DE
"""

import asyncio
import argparse
import json
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
DEFAULT_VOICE  = "de-DE-ConradNeural"
DEFAULT_RATE   = "-10%"
DEFAULT_VOLUME = "+0%"


# ---------------------------------------------------------------------------
# Segment types & tag sets
# ---------------------------------------------------------------------------
SEG_HEADING   = "heading"
SEG_PARAGRAPH = "paragraph"

KEEP_TAGS    = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "blockquote"}
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
SKIP_TAGS    = {"nav", "script", "style", "head"}

# TOC chapter title detection (case-insensitive)
TOC_TITLE_RE = re.compile(
    r"^\s*(inhalts?verzeichnis|inhalt|contents?|table\s+of\s+contents?|toc|sommaire|índice)\s*$",
    re.IGNORECASE,
)

# SSML break durations
HEADING_PRE_BREAK  = "1500ms"
HEADING_POST_BREAK = "800ms"
PARA_BREAK         = "400ms"


# ---------------------------------------------------------------------------
# Text / segment helpers
# ---------------------------------------------------------------------------

def sanitize_text(text: str) -> str:
    """Remove control characters and normalize whitespace."""
    text = text.replace("\ufffd", "")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_toc_title(title: str) -> bool:
    """Return True if the title matches common TOC heading patterns."""
    return bool(TOC_TITLE_RE.match(title.strip()))


def is_toc_content(text: str) -> bool:
    """Heuristic: True if >40% of non-empty lines look like TOC entries (end with page numbers)."""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) < 5:
        return False
    toc_lines = sum(1 for l in lines if re.search(r"[\.\s]{3,}\d+\s*$", l))
    return toc_lines / len(lines) > 0.4


def segments_to_plain_text(segments: list[dict]) -> str:
    """Collapse segments to plain text (used for length checks and TOC detection)."""
    return " ".join(seg["text"] for seg in segments)


def segments_to_tts_text(segments: list[dict]) -> str:
    """
    Build TTS-ready text with punctuation-based prosodic pauses (Option 1).

    NOTE: edge-tts XML-escapes the text before embedding it in SSML, so literal
    <break> tags would be read aloud. Instead we use punctuation-based prosodic
    pauses that the TTS engine interprets naturally:
      • Before a heading  → "…" (ellipsis, ~1 s pause)
      • After  a heading  → "." (period, short pause before body text)
      • Between paragraphs→ text already ends with punctuation; one space suffices
    """
    _SENTENCE_END = frozenset('.!?…\u201d\u00bb')   # ."!?…"»

    parts: list[str] = []
    for i, seg in enumerate(segments):
        text = seg["text"].strip()
        if not text:
            continue

        if seg["type"] == SEG_HEADING:
            if i > 0:
                parts.append(" \u2026 ")      # " … " – long prosodic pause before heading
            # Ensure heading ends with a sentence-boundary so TTS pauses after it
            if text[-1] not in _SENTENCE_END:
                text += "."
            parts.append(text)
            parts.append(" ")                 # brief gap before body text
        else:
            if i > 0:
                prev_is_heading = segments[i - 1]["type"] == SEG_HEADING
                if not prev_is_heading:
                    # Add a short pause between paragraphs via sentence boundary
                    if parts and parts[-1][-1] not in _SENTENCE_END:
                        parts.append(".")
                    parts.append(" ")
                else:
                    parts.append(" ")
            parts.append(text)

    return "".join(parts)


def segments_to_fallback_text(segments: list[dict]) -> str:
    """
    Option 3 fallback: spoken placeholder sentences at structural boundaries.
    Used when the primary synthesis fails at runtime.
    """
    parts: list[str] = []
    for i, seg in enumerate(segments):
        if i > 0:
            parts.append("Neues Kapitel." if seg["type"] == SEG_HEADING else "Weiter.")
        parts.append(seg["text"])
    return " ".join(parts)


def chunk_segments(segments: list[dict], max_chars: int = 4900) -> list[list[dict]]:
    """
    Group segments into chunks whose combined text fits within max_chars.
    Splits within a segment only when a single segment exceeds max_chars.
    """
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_len = 0

    for seg in segments:
        seg_len = len(seg["text"])

        if seg_len > max_chars:
            # Flush current chunk first
            if current:
                chunks.append(current)
                current, current_len = [], 0
            # Hard-split the oversized segment at sentence boundaries
            sentences = re.split(r"(?<=[.!?])\s+", seg["text"])
            buf = ""
            for sent in sentences:
                if len(buf) + len(sent) + 1 <= max_chars:
                    buf = (buf + " " + sent).strip()
                else:
                    if buf:
                        chunks.append([{"type": seg["type"], "text": buf}])
                    while len(sent) > max_chars:
                        chunks.append([{"type": seg["type"], "text": sent[:max_chars]}])
                        sent = sent[max_chars:]
                    buf = sent
            if buf:
                chunks.append([{"type": seg["type"], "text": buf}])

        elif current_len + seg_len + 1 > max_chars:
            chunks.append(current)
            current, current_len = [seg], seg_len

        else:
            current.append(seg)
            current_len += seg_len + 1

    if current:
        chunks.append(current)

    return chunks


# ---------------------------------------------------------------------------
# EPUB helpers
# ---------------------------------------------------------------------------

def html_to_segments(html_bytes: bytes) -> list[dict]:
    """Extract structured segments (headings + paragraphs) from an EPUB HTML chunk."""
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(SKIP_TAGS):
        tag.decompose()

    segments: list[dict] = []
    for element in soup.find_all(KEEP_TAGS):
        text = sanitize_text(element.get_text(" ", strip=True))
        if not text:
            continue
        seg_type = SEG_HEADING if element.name in HEADING_TAGS else SEG_PARAGRAPH
        segments.append({"type": seg_type, "text": text})

    # Fallback: no recognised tags → treat full text as one paragraph
    if not segments:
        text = sanitize_text(soup.get_text(" ", strip=True))
        if text:
            segments.append({"type": SEG_PARAGRAPH, "text": text})

    return segments


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
        start_page: int | None = None,      # PDF: first page to include (1-based)
        end_page: int | None = None,        # PDF: last page to include (1-based, inclusive)
        skip_chapters: set[int] | None = None,  # EPUB/TXT: doc-order indices to skip
    ):
        self.epub_path    = Path(epub_path)
        self.output_dir   = (
            Path(output_dir) if output_dir
            else self.epub_path.parent / self.epub_path.stem
        )
        self.voice         = voice
        self.rate          = rate
        self.volume        = volume
        self.skip_short    = skip_short
        self.max_chapters  = max_chapters
        self.merge         = merge
        self.start_page    = start_page
        self.end_page      = end_page
        self.skip_chapters = skip_chapters or set()

    # ------------------------------------------------------------------
    @staticmethod
    def _clean_title(raw: str) -> str:
        """Strip author prefix/suffix from a raw title string."""
        for sep in (" - ", " – ", ": "):
            if sep in raw:
                raw = raw.split(sep, 1)[-1].strip()
                break
        return re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()

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

        items       = {item.get_id(): item for item in book.get_items()}
        chapters:   list[dict] = []
        skipped_toc = 0
        doc_idx     = -1   # sequential index among ITEM_DOCUMENT spine items

        for spine_id, _ in book.spine:
            item = items.get(spine_id)
            if item is None or item.get_type() != ebooklib.ITEM_DOCUMENT:
                continue
            doc_idx += 1
            if doc_idx in self.skip_chapters:
                continue

            segments = html_to_segments(item.get_content())
            if not segments:
                continue

            plain = segments_to_plain_text(segments)
            if len(plain) < self.skip_short:
                continue

            # Determine chapter title from first heading or item name
            soup     = BeautifulSoup(item.get_content(), "html.parser")
            h_tag    = soup.find(["h1", "h2", "h3"])
            ch_title = h_tag.get_text(strip=True) if h_tag else item.get_name()

            # Skip TOC chapters (nav tags are already removed by html_to_segments;
            # this catches TOC chapters that appear in the spine)
            if is_toc_title(ch_title) or is_toc_content(plain):
                skipped_toc += 1
                print(f"  ⏭  Inhaltsverzeichnis übersprungen: {ch_title!r}")
                continue

            chapters.append({"title": ch_title, "segments": segments})

        if skipped_toc:
            print(f"  ℹ  {skipped_toc} Inhaltsverzeichnis-Kapitel übersprungen.\n")

        return chapters, title

    # ------------------------------------------------------------------
    def _load_pdf(self) -> tuple[list[dict], str]:
        """
        Read a PDF and return (chapters, clean title).
        Attempts heading detection via font-size comparison (Option 1).
        Falls back to paragraph-only structure with a warning if sizes are uniform.
        """
        import fitz  # pymupdf

        doc   = fitz.open(str(self.epub_path))
        raw   = (doc.metadata.get("title") or "").strip() or self.epub_path.stem
        title = self._clean_title(raw) or self.epub_path.stem

        # --- Detect heading font-size threshold ---
        font_sizes: list[float] = []
        for page in doc:
            for block in page.get_text("dict")["blocks"]:
                if block.get("type") != 0:   # 0 = text block
                    continue
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        if span.get("text", "").strip():
                            font_sizes.append(span["size"])

        has_heading_detection = False
        heading_threshold     = float("inf")
        if font_sizes:
            sorted_sizes = sorted(font_sizes)
            median_size  = sorted_sizes[len(sorted_sizes) // 2]
            max_size     = sorted_sizes[-1]
            # Only use heading detection when there is meaningful size variation (≥15%)
            if max_size > median_size * 1.15:
                heading_threshold     = median_size * 1.15
                has_heading_detection = True

        if not has_heading_detection:
            print(
                "  ℹ  Hinweis (PDF): Überschriften konnten nicht per Schriftgröße erkannt werden –\n"
                "     alle Textblöcke werden als Absätze behandelt (SSML-Absatz-Pausen aktiv).\n"
                "     Tipp: Option 3 (Platzhalter-Sätze) wäre eine Alternative für\n"
                "     noch klarere Strukturierung bei uniform gesetzten PDFs.",
                file=sys.stderr,
            )

        # --- Apply user-selected page range (1-based, inclusive) ---
        first_page = max(1, self.start_page or 1)
        last_page  = min(len(doc), self.end_page or len(doc))

        # --- Extract segments page by page, group into chapters ---
        TARGET    = 15_000
        chapters: list[dict] = []
        cur_segs: list[dict] = []
        cur_len   = 0
        page_start = first_page
        skipped_toc = 0

        for i, page in enumerate(doc, start=1):
            if i < first_page or i > last_page:
                continue
            if has_heading_detection:
                for block in page.get_text("dict")["blocks"]:
                    if block.get("type") != 0:
                        continue
                    block_parts: list[str] = []
                    is_heading = False
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            t = sanitize_text(span.get("text", ""))
                            if t:
                                block_parts.append(t)
                                if span["size"] >= heading_threshold:
                                    is_heading = True
                    block_text = " ".join(block_parts).strip()
                    if block_text:
                        seg_type = SEG_HEADING if is_heading else SEG_PARAGRAPH
                        cur_segs.append({"type": seg_type, "text": block_text})
                        cur_len += len(block_text)
            else:
                page_text = sanitize_text(page.get_text())
                if page_text:
                    cur_segs.append({"type": SEG_PARAGRAPH, "text": page_text})
                    cur_len += len(page_text)

            at_end = (i == len(doc))
            if cur_len >= TARGET or at_end:
                plain = segments_to_plain_text(cur_segs)
                label = f"Seiten {page_start}–{i}" if page_start < i else f"Seite {i}"
                if len(plain) >= self.skip_short:
                    if is_toc_content(plain):
                        skipped_toc += 1
                        print(f"  ⏭  Inhaltsverzeichnis übersprungen: {label}")
                    else:
                        chapters.append({"title": label, "segments": cur_segs})
                cur_segs   = []
                cur_len    = 0
                page_start = i + 1

        doc.close()
        if skipped_toc:
            print(f"  ℹ  {skipped_toc} Inhaltsverzeichnis-Seite(n) übersprungen.\n")

        return chapters, title

    # ------------------------------------------------------------------
    def _load_txt(self) -> tuple[list[dict], str]:
        """
        Read a plain-text file and return (sections as chapters, clean title).
        Detects headings via common keywords; splits on double-newlines for paragraphs.
        """
        raw_title = self._clean_title(self.epub_path.stem) or self.epub_path.stem
        content   = self.epub_path.read_text(encoding="utf-8", errors="replace")

        HEADING_RE = re.compile(
            r"(?m)^(?:Kapitel|Chapter|Teil|Part|Abschnitt|Section|Epilog|Prolog|Vorwort|Nachwort)\s+\S.*$"
        )

        # Build flat list of segments, marking headings
        segments_raw: list[dict] = []
        last_end = 0

        for m in HEADING_RE.finditer(content):
            # Paragraphs before this heading
            before = content[last_end:m.start()]
            for para in re.split(r"\n{2,}", before):
                text = sanitize_text(para)
                if len(text) >= self.skip_short:
                    segments_raw.append({"type": SEG_PARAGRAPH, "text": text})
            # The heading line
            heading_text = sanitize_text(m.group(0))
            if heading_text:
                segments_raw.append({"type": SEG_HEADING, "text": heading_text})
            last_end = m.end()

        # Remaining text after the last heading
        for para in re.split(r"\n{2,}", content[last_end:]):
            text = sanitize_text(para)
            if len(text) >= self.skip_short:
                segments_raw.append({"type": SEG_PARAGRAPH, "text": text})

        # No headings found → split on paragraph breaks only
        if not segments_raw:
            for para in re.split(r"\n{2,}", content):
                text = sanitize_text(para)
                if len(text) >= self.skip_short:
                    segments_raw.append({"type": SEG_PARAGRAPH, "text": text})

        # Further fallback: no paragraphs → equal-size chunks
        if not segments_raw:
            full  = sanitize_text(content)
            CHUNK = 20_000
            for start in range(0, len(full), CHUNK):
                text = full[start:start + CHUNK]
                if len(text) >= self.skip_short:
                    segments_raw.append({"type": SEG_PARAGRAPH, "text": text})

        # Group flat segments into logical chapters (split at heading boundaries)
        chapters:    list[dict] = []
        current:     list[dict] = []
        skipped_toc  = 0
        sec_idx      = -1   # sequential section index (for skip_chapters)

        def flush_chapter(segs: list[dict]) -> None:
            nonlocal skipped_toc, sec_idx
            if not segs:
                return
            sec_idx += 1
            if sec_idx in self.skip_chapters:
                return
            plain            = segments_to_plain_text(segs)
            title_candidate  = (
                segs[0]["text"][:80]
                if segs[0]["type"] == SEG_HEADING
                else f"Abschnitt {len(chapters) + 1}"
            )
            if is_toc_title(title_candidate) or is_toc_content(plain):
                skipped_toc += 1
                print(f"  ⏭  Inhaltsverzeichnis übersprungen: {title_candidate!r}")
            else:
                chapters.append({"title": title_candidate, "segments": segs})

        for seg in segments_raw:
            if seg["type"] == SEG_HEADING and current:
                flush_chapter(current)
                current = [seg]
            else:
                current.append(seg)
        flush_chapter(current)

        if skipped_toc:
            print(f"  ℹ  {skipped_toc} Inhaltsverzeichnis-Abschnitt(e) übersprungen.\n")

        return chapters, raw_title

    # ------------------------------------------------------------------
    async def _synthesise_chunk(self, segments: list[dict], out_path: Path) -> None:
        """
        Synthesise a segment chunk to MP3.
        Primary:  SSML <break> tags (Option 1).
        Fallback: spoken placeholder sentences (Option 3) if SSML fails.
        """
        ssml_text = segments_to_tts_text(segments)
        try:
            communicate = edge_tts.Communicate(ssml_text, self.voice, rate=self.rate, volume=self.volume)
            await communicate.save(str(out_path))
            if out_path.exists() and out_path.stat().st_size < 1024:
                out_path.unlink()
                raise RuntimeError("Edge-TTS lieferte eine leere/korrupte Datei.")
        except Exception as exc:
            # --- Option 3 fallback ---
            print(
                f"\n  ⚠  SSML fehlgeschlagen ({exc}).\n"
                f"     Hinweis: Wechsle zu Option 3 (Platzhalter-Sätze) …",
                end="",
                flush=True,
            )
            fallback = segments_to_fallback_text(segments)
            communicate = edge_tts.Communicate(fallback, self.voice, rate=self.rate, volume=self.volume)
            await communicate.save(str(out_path))
            if out_path.exists() and out_path.stat().st_size < 1024:
                out_path.unlink()
                raise RuntimeError("Edge-TTS: Auch Fallback-Text (Option 3) führte zu einer leeren Datei.")

    # ------------------------------------------------------------------
    async def convert(self) -> None:
        """Main entry point – convert all chapters to MP3 files."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        chapters, book_title = self._load_chapters()
        if self.max_chapters:
            chapters = chapters[:self.max_chapters]

        if not chapters:
            print("⚠  Keine lesbaren Kapitel gefunden – ist die Datei gültig?", file=sys.stderr)
            sys.exit(1)

        safe_book = re.sub(r'[\\/:*?"<>|]', "_", book_title)[:80]

        # Pre-compute all (chapter_idx, split_idx, segment_chunk) tuples
        jobs: list[tuple[int, int, list[dict]]] = []
        for ch_idx, chapter in enumerate(chapters, start=1):
            for sp_idx, seg_chunk in enumerate(chunk_segments(chapter["segments"]), start=1):
                jobs.append((ch_idx, sp_idx, seg_chunk))

        total = len(jobs)
        print(f"📖  Buch:      {book_title}")
        print(f"🔊  Stimme:    {self.voice}  |  Rate: {self.rate}")
        print(f"📂  Ausgabe:   {self.output_dir}")
        print(f"📑  Kapitel:   {len(chapters)}  |  Splits: {total}\n")

        produced: list[Path] = []

        for counter, (ch_idx, sp_idx, seg_chunk) in enumerate(jobs, start=1):
            filename = self.output_dir / f"{ch_idx:03d}_{sp_idx:03d}_{safe_book}.mp3"
            chars    = sum(len(s["text"]) for s in seg_chunk)

            print(f"  [{counter:>3}/{total}]  {ch_idx:03d}_{sp_idx:03d}  ({chars:,} Zeichen) … ", end="", flush=True)

            try:
                await self._synthesise_chunk(seg_chunk, filename)
                produced.append(filename)
                print("✓")
            except Exception as exc:
                print(f"✗  FEHLER: {exc}")

        if self.merge and produced:
            merged = self.output_dir / f"{safe_book}.mp3"
            print(f"\n🔗  Zusammenführen von {len(produced)} Datei(en) → {merged.name} … ", end="", flush=True)
            with open(merged, "wb") as fout:
                for part in produced:
                    fout.write(part.read_bytes())
                    part.unlink()
            print("✓")

        print(f"\n✅  Fertig! Dateien gespeichert in: {self.output_dir}")


# ---------------------------------------------------------------------------
# Document structure extraction (for GUI preview)
# ---------------------------------------------------------------------------

def get_structure_cmd(file_path: str) -> None:
    """
    Extract document structure (chapters / pages) and print JSON to stdout.
    Used by the Electron front-end via --get-structure for the preview modal.
    """
    path = Path(file_path)
    if not path.exists():
        print(json.dumps({"error": f"File not found: {file_path}"}))
        return
    ext = path.suffix.lower()
    try:
        if ext == ".pdf":
            _structure_pdf(path)
        elif ext == ".epub":
            _structure_epub(path)
        elif ext == ".txt":
            _structure_txt(path)
        else:
            print(json.dumps({"error": f"Unsupported: {ext}"}))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))


def _structure_pdf(path: Path, max_thumbs: int = 40) -> None:
    import fitz, base64 as _b64

    doc   = fitz.open(str(path))
    title = (doc.metadata.get("title") or "").strip() or path.stem
    total = len(doc)

    pages = []
    for i in range(min(max_thumbs, total)):
        page  = doc[i]
        rect  = page.rect
        scale = 120 / max(rect.width, 1)
        pix   = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        thumb = _b64.b64encode(pix.tobytes("png")).decode()
        plain = sanitize_text(page.get_text())
        pages.append({
            "page":         i + 1,
            "thumbnail":    thumb,
            "textPreview":  plain[:200],
            "looksLikeToc": is_toc_content(plain),
            "looksLikeCover": i == 0 and len(plain) < 300,
        })

    doc.close()
    print(json.dumps({
        "type":       "pdf",
        "title":      title,
        "totalPages": total,
        "thumbCount": len(pages),
        "pages":      pages,
    }), flush=True)


def _structure_epub(path: Path) -> None:
    EpubConverter._patch_ebooklib()
    book  = epub.read_epub(str(path), options={"ignore_ncx": True})
    meta  = book.get_metadata("DC", "title")
    raw   = meta[0][0].strip() if meta else path.stem
    title = EpubConverter._clean_title(raw) or path.stem

    items    = {item.get_id(): item for item in book.get_items()}
    chapters = []
    doc_idx  = -1

    for spine_id, _ in book.spine:
        item = items.get(spine_id)
        if item is None or item.get_type() != ebooklib.ITEM_DOCUMENT:
            continue
        doc_idx += 1
        segments = html_to_segments(item.get_content())
        plain    = segments_to_plain_text(segments)
        soup     = BeautifulSoup(item.get_content(), "html.parser")
        h_tag    = soup.find(["h1", "h2", "h3"])
        ch_title = h_tag.get_text(strip=True) if h_tag else item.get_name()
        chapters.append({
            "index":    doc_idx,
            "title":    ch_title,
            "chars":    len(plain),
            "isToc":    is_toc_title(ch_title) or is_toc_content(plain),
        })

    print(json.dumps({
        "type":     "epub",
        "title":    title,
        "chapters": chapters,
    }), flush=True)


def _structure_txt(path: Path) -> None:
    content = path.read_text(encoding="utf-8", errors="replace")
    title   = EpubConverter._clean_title(path.stem) or path.stem

    HEADING_RE = re.compile(
        r"(?m)^(?:Kapitel|Chapter|Teil|Part|Abschnitt|Section|Epilog|Prolog|Vorwort|Nachwort)\s+\S.*$"
    )
    parts   = re.split(r"\x00|\n{3,}", HEADING_RE.sub("\x00\\g<0>", content))
    sections = []
    for i, part in enumerate(parts):
        text = sanitize_text(part)
        if len(text) < 30:
            continue
        first_line = part.strip().splitlines()[0][:80] if part.strip() else f"Abschnitt {i+1}"
        sections.append({
            "index": i,
            "title": sanitize_text(first_line),
            "chars": len(text),
            "isToc": is_toc_title(first_line) or is_toc_content(text),
        })

    print(json.dumps({
        "type":     "txt",
        "title":    title,
        "sections": sections,
    }), flush=True)


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

def _extract_sample(path: Path, max_chars: int = 6000) -> str:
    """Extract a plain-text sample from EPUB / PDF / TXT for language detection."""
    ext = path.suffix.lower()
    try:
        if ext == ".txt":
            return sanitize_text(
                path.read_text(encoding="utf-8", errors="replace")
            )[:max_chars]

        if ext == ".epub":
            from ebooklib import epub as _epub
            book = _epub.read_epub(str(path), options={"ignore_ncx": True})
            buf = ""
            for item in book.get_items():
                if item.get_type() != ebooklib.ITEM_DOCUMENT:
                    continue
                soup = BeautifulSoup(item.get_content(), "html.parser")
                for tag in soup(["nav", "script", "style"]):
                    tag.decompose()
                buf += " " + soup.get_text(" ", strip=True)
                if len(buf) >= max_chars:
                    break
            return sanitize_text(buf)[:max_chars]

        if ext == ".pdf":
            import fitz
            doc = fitz.open(str(path))
            buf = ""
            for page in doc:
                buf += page.get_text()
                if len(buf) >= max_chars:
                    break
            doc.close()
            return sanitize_text(buf)[:max_chars]

    except Exception:
        pass
    return ""


def _detect_language_heuristic(text: str) -> tuple[str, float]:
    """
    Lightweight heuristic language detection based on character sets and
    common function words. Returns (language_code, confidence 0–1).
    """
    sample = text[:4000].lower()
    words  = set(re.findall(r"\b[a-zäöüßàâçéèêëîïôùûüœæñ]{2,}\b", sample))

    LANG_WORDS: dict[str, list[str]] = {
        "de": ["und", "der", "die", "das", "ist", "nicht", "mit", "ich", "auf", "auch", "ein", "eine", "des", "dem", "den"],
        "en": ["the", "and", "is", "are", "was", "for", "this", "that", "with", "have", "not", "from", "they", "which"],
        "fr": ["les", "des", "une", "est", "que", "qui", "dans", "pour", "pas", "par", "sur", "avec", "mais", "ont"],
        "es": ["los", "las", "una", "que", "con", "por", "para", "como", "más", "pero", "del", "son", "sus", "este"],
        "it": ["che", "non", "una", "della", "sono", "come", "per", "con", "del", "alla", "nel", "questo", "più"],
        "nl": ["van", "het", "een", "dat", "zijn", "ook", "niet", "voor", "met", "door", "maar", "bij", "wordt"],
        "pt": ["que", "não", "uma", "com", "por", "para", "mais", "dos", "nas", "seu", "sua", "mas", "pelo"],
        "ru": [],  # Cyrillic → detected separately
        "zh": [],  # CJK → detected separately
        "ja": [],  # CJK / Kana → detected separately
    }

    # CJK / Cyrillic shortcuts
    if re.search(r"[\u4e00-\u9fff]", text[:2000]):
        # Distinguish Japanese (Hiragana/Katakana) from Chinese
        if re.search(r"[\u3040-\u30ff]", text[:2000]):
            return "ja", 0.85
        return "zh", 0.85
    if re.search(r"[\u0400-\u04ff]", text[:2000]):
        return "ru", 0.85

    scores: dict[str, int] = {}
    for lang, kw in LANG_WORDS.items():
        if not kw:
            continue
        scores[lang] = sum(1 for w in kw if w in words)

    if not scores or max(scores.values()) == 0:
        return "en", 0.3   # safe default

    best    = max(scores, key=scores.get)
    total   = sum(scores.values())
    conf    = round(scores[best] / total, 2) if total else 0.3
    return best, min(conf, 0.95)


def detect_language_cmd(file_path: str) -> None:
    """
    Detect the language of a file and print JSON to stdout.
    Used by the Electron front-end via --detect-language.
    Output: {"language": "de", "confidence": 0.9, "method": "langdetect"|"heuristic"|"error"}
    """
    path = Path(file_path)
    if not path.exists():
        print(json.dumps({"language": None, "error": f"File not found: {file_path}"}))
        return

    sample = _extract_sample(path)
    if not sample or len(sample) < 30:
        print(json.dumps({"language": None, "error": "Not enough text to detect language"}))
        return

    # Try langdetect first (more accurate)
    try:
        from langdetect import detect_langs
        results = detect_langs(sample)
        if results:
            best     = results[0]
            lang     = best.lang.split("-")[0]   # "zh-cn" → "zh"
            conf     = round(best.prob, 2)
            print(json.dumps({"language": lang, "confidence": conf, "method": "langdetect"}))
            return
    except ImportError:
        pass   # langdetect not installed → fall through to heuristic
    except Exception:
        pass

    # Heuristic fallback
    lang, conf = _detect_language_heuristic(sample)
    print(json.dumps({"language": lang, "confidence": conf, "method": "heuristic"}))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def list_voices_cmd(filter_str: str = "") -> None:
    voices   = await edge_tts.list_voices()
    filtered = [v for v in voices if filter_str.lower() in v["ShortName"].lower()] if filter_str else voices
    print(f"{'Short Name':<35}  {'Locale':<10}  Gender")
    print("-" * 60)
    for v in filtered:
        print(f"{v['ShortName']:<35}  {v['Locale']:<10}  {v['Gender']}")
    print(f"\n{len(filtered)} voice(s) found.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lightweight EPUB / PDF / TXT → MP3 converter powered by edge-tts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python epub_to_voice.py book.epub
  python epub_to_voice.py book.epub -o D:/Audiobooks/MyBook -v en-US-AriaNeural -r "+15%"
  python epub_to_voice.py --list-voices de
        """,
    )
    parser.add_argument("epub",            nargs="?", help="Path to the .epub, .pdf, or .txt file")
    parser.add_argument("-o", "--output",  help="Output directory (default: <stem>/)")
    parser.add_argument("-v", "--voice",   default=DEFAULT_VOICE, help=f"TTS voice (default: {DEFAULT_VOICE})")
    parser.add_argument("-r", "--rate",    default=DEFAULT_RATE,  help='Speech rate, e.g. "+15%%" or "-10%%"')
    parser.add_argument("--volume",        default=DEFAULT_VOLUME, help='Volume, e.g. "+10%%"')
    parser.add_argument("--skip-short",    type=int, default=60,
                        help="Skip chapters/sections with fewer than N characters (default: 60)")
    parser.add_argument("--max-chapters",  type=int, default=None,
                        help="Convert only the first N chapters (e.g. 3 for a preview)")
    parser.add_argument("--merge",         action="store_true",
                        help="Concatenate all splits into a single MP3 file")
    parser.add_argument("--list-voices",      nargs="?", const="", metavar="FILTER",
                        help='List available voices, optionally filtered (e.g. --list-voices de)')
    parser.add_argument("--detect-language",  action="store_true",
                        help='Detect the language of the input file and print JSON (used by GUI)')
    parser.add_argument("--get-structure",    action="store_true",
                        help='Extract document structure (chapters/pages) and print JSON (used by GUI)')
    parser.add_argument("--start-page",  type=int, default=None,
                        help='PDF: first page to convert (1-based, default: 1)')
    parser.add_argument("--end-page",    type=int, default=None,
                        help='PDF: last page to convert (1-based, inclusive, default: last)')
    parser.add_argument("--skip-chapters", default="",
                        help='Comma-separated doc-order indices of chapters/sections to skip')
    return parser


def main() -> None:
    # On Windows without a console (e.g. spawned from Electron), the default
    # ProactorEventLoop can fail with aiohttp/edge-tts. SelectorEventLoop is safer.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = build_parser()
    args   = parser.parse_args()

    if args.list_voices is not None:
        asyncio.run(list_voices_cmd(args.list_voices))
        return

    if args.detect_language:
        if not args.epub:
            print(json.dumps({"language": None, "error": "No file specified"}))
            sys.exit(1)
        detect_language_cmd(args.epub)
        return

    if args.get_structure:
        if not args.epub:
            print(json.dumps({"error": "No file specified"}))
            sys.exit(1)
        get_structure_cmd(args.epub)
        return

    if not args.epub:
        parser.print_help()
        sys.exit(1)

    SUPPORTED = {".epub", ".pdf", ".txt"}
    epub_path = Path(args.epub)
    if not epub_path.exists():
        print(f"Fehler: Datei nicht gefunden: {epub_path}", file=sys.stderr)
        sys.exit(1)
    if epub_path.suffix.lower() not in SUPPORTED:
        print(f"Fehler: Nicht unterstütztes Format '{epub_path.suffix}'. Unterstützt: {', '.join(SUPPORTED)}", file=sys.stderr)
        sys.exit(1)

    skip_set = {int(x) for x in args.skip_chapters.split(",") if x.strip().isdigit()}

    converter = EpubConverter(
        epub_path     = epub_path,
        output_dir    = args.output,
        voice         = args.voice,
        rate          = args.rate,
        volume        = args.volume,
        skip_short    = args.skip_short,
        max_chapters  = args.max_chapters,
        merge         = args.merge,
        start_page    = args.start_page,
        end_page      = args.end_page,
        skip_chapters = skip_set,
    )
    asyncio.run(converter.convert())


if __name__ == "__main__":
    main()
