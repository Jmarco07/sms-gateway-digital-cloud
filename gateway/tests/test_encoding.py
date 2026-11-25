"""Encoding detection and segment counting (no database needed)."""
import pytest

from app.encoding import GSM7, UCS2, analyze, detect_encoding


@pytest.mark.parametrize("text,expected", [
    ("Hello world 123", GSM7),
    ("Price: 5€ [ok] {x} ~ | ^ \\", GSM7),   # extension table
    ("Café ñ ü à é", GSM7),                   # accented letters that ARE in GSM 03.38
    ("çava", UCS2),                           # lower-case c-cedilla is NOT in GSM 03.38
    ("backtick `", UCS2),                     # neither is the backtick
    ("আপনার কোড 1234", UCS2),                 # Bangla
    ("Great job 😀", UCS2),                   # emoji
])
def test_detect_encoding(text, expected):
    assert detect_encoding(text) == expected


@pytest.mark.parametrize("length,segments", [(1, 1), (160, 1), (161, 2), (306, 2), (307, 3), (459, 3), (460, 4)])
def test_gsm7_segment_boundaries(length, segments):
    result = analyze("a" * length)
    assert (result.encoding, result.length, result.segments) == (GSM7, length, segments)


def test_gsm7_extension_characters_count_double():
    # 80 x '{' = 160 septets: exactly one segment. One more makes it 162: two segments.
    one = analyze("{" * 80)
    assert (one.encoding, one.length, one.segments) == (GSM7, 160, 1)
    two = analyze("{" * 81)
    assert (two.length, two.segments) == (162, 2)
    assert analyze("€").length == 2
    assert analyze("[a]").length == 5


def test_extension_character_is_not_split_across_segments():
    # 152 'a' + '{' (2) = 154 septets: it does not fit in the 153 of segment 1, so the '{'
    # moves whole to segment 2. Total (164 with 10 more) is still 2 segments.
    text = "a" * 152 + "{" + "a" * 10
    result = analyze(text)
    assert result.length == 164 and result.segments == 2


@pytest.mark.parametrize("length,segments", [(1, 1), (70, 1), (71, 2), (134, 2), (135, 3)])
def test_ucs2_bangla_segment_boundaries(length, segments):
    result = analyze("ক" * length)
    assert (result.encoding, result.length, result.segments) == (UCS2, length, segments)


def test_ucs2_emoji_counts_as_two_code_units():
    one = analyze("😀" * 35)            # 70 UTF-16 code units
    assert (one.encoding, one.length, one.segments) == (UCS2, 70, 1)
    assert analyze("😀" * 36).segments == 2


def test_ucs2_surrogate_pair_is_not_split():
    # 67 emoji = 134 code units. Dividing 134 by 67 would say 2 segments, but a segment holds
    # 33 whole emoji (66 units), so the real answer is 3.
    result = analyze("😀" * 67)
    assert result.length == 134 and result.segments == 3


def test_empty_text_is_rejected():
    with pytest.raises(ValueError):
        analyze("")
