#!/usr/bin/env python3
"""
Smoke-Test fuer die Edge-TTS-Synthese mit der deutschen Conrad-Stimme.

Synthetisiert einen Testsatz ueber den echten Produktions-Code-Pfad
(EpubConverter._synthesise_chunk_edge) und schreibt das Ergebnis nach
test_conrad.mp3 im Projekt-Root.

Aufruf:
    python tests/test_conrad.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from epub_to_voice import DEFAULT_VOICE, SEG_PARAGRAPH, EpubConverter

TEST_SENTENCE = (
    "Robert Cavendish, der siebenundzwanzigste Earl of Thornfield, "
    "stand auf der Brücke und wartete, dass der Verkehr sich löste."
)

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "test_conrad.mp3"


async def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    converter = EpubConverter(
        epub_path="dummy.txt",  # nicht gelesen, nur fuer Pfad-Defaults benoetigt
        tts_engine="edge",
        voice=DEFAULT_VOICE,
    )
    segments = [{"type": SEG_PARAGRAPH, "text": TEST_SENTENCE}]

    print(f"Synthetisiere Testsatz mit Stimme '{converter.voice}' ...")
    await converter._synthesise_chunk_edge(segments, OUTPUT_PATH)

    if OUTPUT_PATH.exists() and OUTPUT_PATH.stat().st_size > 1024:
        size_kb = OUTPUT_PATH.stat().st_size / 1024
        print(f"OK: {OUTPUT_PATH} ({size_kb:.1f} KB)")
    else:
        print("FEHLER: Ausgabedatei fehlt oder ist zu klein.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
