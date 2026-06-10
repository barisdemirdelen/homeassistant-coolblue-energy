"""
util.py

Shared utilities for the Coolblue Energy integration.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BeforeValidator


def coerce_float(v: Any) -> float:
    """
    Coerce API float fields that may arrive as ``None`` or a ``'$-0'``-style
    RSC wire-format-prefixed string (Next.js encodes ``-0`` as ``'$-0'``).
    """
    match v:
        case None:
            return 0.0
        case str():
            try:
                return float(v.replace("$", "").strip())
            except ValueError:
                return 0.0
        case _:
            return float(v)


CoercedFloat = Annotated[float, BeforeValidator(coerce_float)]
"""
A ``float`` type that accepts ``None`` (→ ``0.0``) and RSC ``'$…'``-prefixed
numeric strings in addition to plain numbers.
"""
