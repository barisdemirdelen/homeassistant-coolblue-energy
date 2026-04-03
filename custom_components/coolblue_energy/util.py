"""
util.py

Shared utilities for the Coolblue Energy integration.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BeforeValidator


def coerce_float(v: object) -> float:
    """
    Coerce API float fields that may arrive as ``None`` or a ``'$-0'``-style
    RSC wire-format-prefixed string (Next.js encodes ``-0`` as ``'$-0'``).
    """
    if v is None:
        return 0.0
    if isinstance(v, str):
        cleaned = v.replace("$", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return float(v)


CoercedFloat = Annotated[float, BeforeValidator(coerce_float)]
"""
A ``float`` type that accepts ``None`` (→ ``0.0``) and RSC ``'$…'``-prefixed
numeric strings in addition to plain numbers.
"""
