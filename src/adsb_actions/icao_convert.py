"""Conversion tools for ICAO hex codes found in ADS-B messages."""
from icao_nnumber_converter_us import icao_to_n

def icao_to_n_or_c(hexstr: str) -> str:
    """Given ICAO hex code, convert to N- or C- tail number."""
    if not str:
        return None

    if hexstr.upper().startswith('C'):
        result = icao_to_c(hexstr, 'C-F', 0xC00001, 0x44A9, 26*26, 26)
        if not result:
            result = icao_to_c(hexstr, 'C-G', 0xC044A9, 0xFBB56, 26*26, 26)
        return result
    elif hexstr.upper().startswith('A'):
        return icao_to_n(hexstr)
    return None

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

def icao_to_c(hexstr : str, prefix : str, start : int,
              span : int, stride1 : int, stride2: int) -> str:
    """Handle Canadian-style ICAO conversion.  Returns None 
    if out of range."""

    if not hexstr.startswith('0x'):
        hexstr = '0x' + hexstr
    hexval = int(hexstr, 16)

    offset = hexval - start
    if offset > span:
        return None

    i1 = offset // stride1
    offset = offset % stride1
    i2 = offset // stride2
    offset = offset % stride2
    i3 = offset
    try:
        return prefix + ALPHABET[i1] + ALPHABET[i2] + ALPHABET[i3]
    except:
        return None