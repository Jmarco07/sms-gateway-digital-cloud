"""SMS encoding detection and segment counting.

GSM-7 (GSM 03.38):
    * 160 septets fit in one segment; a longer message uses 153 septets per segment
      (7 septets are taken by the concatenation header).
    * Characters of the extension table ({ } [ ] ~ \\ ^ | € and form feed) are sent as
      ESC + code, so each one uses 2 septets.
UCS-2:
    * 70 UTF-16 code units in one segment; 67 per segment when concatenated.
    * A character outside the BMP (most emoji) is a surrogate pair = 2 code units.

A multi-part message never splits an extension character or a surrogate pair across
two segments, so we pack character by character instead of dividing the total.
"""
from dataclasses import dataclass

GSM7_BASIC = (
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ"  # 0x00 - 0x1A  (0x1B is the escape code)
    "ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmnopqrstuvwxyzäöñüà"
)
GSM7_EXTENSION = "\f^{}\\[~]|€"

_BASIC = frozenset(GSM7_BASIC)
_EXTENSION = frozenset(GSM7_EXTENSION)

GSM7 = "GSM7"
UCS2 = "UCS2"

_LIMITS = {
    GSM7: (160, 153),
    UCS2: (70, 67),
}


@dataclass(frozen=True)
class Analysis:
    encoding: str
    length: int      # septets (GSM7) or UTF-16 code units (UCS2)
    segments: int


def detect_encoding(text: str) -> str:
    """GSM7 if every character is in the GSM 03.38 basic or extension table, else UCS2."""
    for ch in text:
        if ch not in _BASIC and ch not in _EXTENSION:
            return UCS2
    return GSM7


def _char_units(ch: str, encoding: str) -> int:
    if encoding == GSM7:
        return 2 if ch in _EXTENSION else 1
    return 2 if ord(ch) > 0xFFFF else 1


def analyze(text: str) -> Analysis:
    if not text:
        raise ValueError("text must not be empty")
    encoding = detect_encoding(text)
    single, multi = _LIMITS[encoding]
    sizes = [_char_units(ch, encoding) for ch in text]
    total = sum(sizes)
    if total <= single:
        return Analysis(encoding, total, 1)

    segments, used = 1, 0
    for size in sizes:
        if used + size > multi:  # does not fit: start a new segment
            segments += 1
            used = 0
        used += size
    return Analysis(encoding, total, segments)
