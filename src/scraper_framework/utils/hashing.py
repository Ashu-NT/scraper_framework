import hashlib

def stable_hash(text: str) -> str:
    """Generate a stable hash from text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
