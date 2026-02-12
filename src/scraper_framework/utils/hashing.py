import hashlib
from typing import Any
import unicodedata
import re

def stable_hash(text: str) -> str:
    """Generate a stable hash from text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def normalize_text(
    value: Any,
    *,
    lowercase: bool = True,
    strip: bool = True,
    collapse_whitespace: bool = True,
    normalize_unicode: bool = True,
) -> str:
    """
    Generic text normalizer for stable hashing and comparisons.
    """

    if value is None:
        return ""

    text = str(value)

    # Unicode normalization (very important for scraping)
    if normalize_unicode:
        text = unicodedata.normalize("NFKC", text)

    # Standardize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    if strip:
        text = text.strip()

    if collapse_whitespace:
        text = re.sub(r"\s+", " ", text)

    if lowercase:
        text = text.lower()

    return text