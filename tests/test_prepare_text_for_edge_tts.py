#!/usr/bin/env python3
"""
Unit-Test fuer prepare_text_for_edge_tts() - das Dialog-/Pausen-Preprocessing
vor der Uebergabe an edge-tts.

Aufruf:
    python tests/test_prepare_text_for_edge_tts.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from epub_to_voice import prepare_text_for_edge_tts

FAILURES = 0


def check(label: str, actual: str, expected: str) -> None:
    global FAILURES
    if actual == expected:
        print(f"OK   : {label}")
    else:
        FAILURES += 1
        print(f"FEHLER: {label}")
        print(f"  erwartet: {expected!r}")
        print(f"  erhalten: {actual!r}")


# ---------------------------------------------------------------------------
# Beispieltext aus der Anfrage: durchgehender Dialog ohne echten Satzschluss
# vor "Warum?" - der abschliessende Punkt nach "umgedreht." ist ein echter
# Satzschluss (SCHRITT 1, Regel 2) und behaelt daher seine eigene Pause;
# die beiden dialoglastigen Fragmente drumherum werden verbunden.
# ---------------------------------------------------------------------------
DIALOGUE_INPUT = (
    "„Du schläfst nicht.“,\n"
    "Er hatte sich nicht umgedreht.\n"
    "„Nein, Mylord.“,\n"
    "„Warum?“"
)
DIALOGUE_EXPECTED = (
    "„Du schläfst nicht.“ Er hatte sich nicht umgedreht.\n\n"
    "„Nein, Mylord.“ „Warum?“"
)
check("Dialog ohne Satzschluss wird verbunden", prepare_text_for_edge_tts(DIALOGUE_INPUT), DIALOGUE_EXPECTED)


# ---------------------------------------------------------------------------
# Stilistisches Abschlusskomma nach schliessendem Anfuehrungszeichen entfernen
# ---------------------------------------------------------------------------
COMMA_INPUT = "„Nein, Mylord.“,\n„Warum?“"
COMMA_EXPECTED = "„Nein, Mylord.“ „Warum?“"
check("Stilistisches Abschlusskomma wird entfernt", prepare_text_for_edge_tts(COMMA_INPUT), COMMA_EXPECTED)


# ---------------------------------------------------------------------------
# Kapitelueberschriften bekommen davor/danach eine doppelte Leerzeile
# ---------------------------------------------------------------------------
HEADING_INPUT = "Ende des letzten Kapitels.\nKapitel 3\nDer erste Satz des neuen Kapitels."
HEADING_EXPECTED = (
    "Ende des letzten Kapitels.\n\n\n"
    "Kapitel 3\n\n\n"
    "Der erste Satz des neuen Kapitels."
)
check("Kapitelueberschrift erhaelt doppelte Leerzeile", prepare_text_for_edge_tts(HEADING_INPUT), HEADING_EXPECTED)


# ---------------------------------------------------------------------------
# Sehr kurze Absaetze (< 8 Zeichen) werden immer mit dem vorherigen verbunden
# ---------------------------------------------------------------------------
SHORT_INPUT = "Ein langer Satz, der endet mit einem Punkt.\nJa.\nEin weiterer Satz folgt hier."
SHORT_EXPECTED = (
    "Ein langer Satz, der endet mit einem Punkt. Ja.\n\n"
    "Ein weiterer Satz folgt hier."
)
check("Sehr kurzer Absatz wird immer angehaengt", prepare_text_for_edge_tts(SHORT_INPUT), SHORT_EXPECTED)


# ---------------------------------------------------------------------------
# Absatz ohne Satzschluss wird mit dem naechsten verbunden (Zeilenumbruch-Fix)
# ---------------------------------------------------------------------------
WRAP_INPUT = "Dies ist ein Satz ohne\nAbschluss der weitergeht.\nUnd hier ein neuer, richtiger Satz."
WRAP_EXPECTED = (
    "Dies ist ein Satz ohne Abschluss der weitergeht.\n\n"
    "Und hier ein neuer, richtiger Satz."
)
check("Absatz ohne Satzschluss wird verbunden", prepare_text_for_edge_tts(WRAP_INPUT), WRAP_EXPECTED)


if FAILURES:
    print(f"\n{FAILURES} Test(s) fehlgeschlagen.")
    sys.exit(1)
print("\nAlle Tests erfolgreich.")
