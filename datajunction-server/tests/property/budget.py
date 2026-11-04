"""
How many examples each property runs.

Properties declare a budget for an ordinary PR run; `DJ_PBT_PROFILE=nightly`
multiplies it for longer searches.
"""

import os

SCALE = {"dev": 1, "ci": 1, "nightly": 10}


def profile() -> str:
    default = "ci" if os.environ.get("CI") else "dev"
    return os.environ.get("DJ_PBT_PROFILE", default)


def examples(n: int) -> int:
    return n * SCALE[profile()]
