import base64

__all__ = ["_encode_binary", "_decode_binary"]

def _encode_binary(data: bytes) -> str:
    """Encode binary data to base64 string."""
    return base64.b64encode(data).decode('utf-8')

def _decode_binary(data: str) -> bytes:
    """Decode base64 string to binary data."""
    return base64.b64decode(data)