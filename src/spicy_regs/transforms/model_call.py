"""Where this host's Gemini key comes from. The call itself is spicy-docs'.

What stays here is the one thing that is this application's and not the
library's: **which environment variable holds the key**. A hosting application
supplies the key; spicy-docs never reads the environment for one. The adapter
joining ``ModelCall`` to ``spicy_docs.extraction.gemini.GeminiClient`` lives
upstream as ``spicy_docs.interpretation.gemini_call``, beside both halves it
joins and with its behaviour proved in that package's tests; a local copy can
only drift, and 0.22.0's ``response_schema`` argument made the old
``call(*, model, prompt)`` signature raise ``TypeError`` on every call.
"""

from __future__ import annotations

import os

#: Checked in order; the first non-empty value wins. A run with none of them
#: set skips the model-backed tables rather than failing.
GEMINI_KEY_ENV_VARS = ("GEMINI_API_KEY", "GOOGLE_API_KEY")


def resolve_gemini_key() -> str | None:
    """The first Gemini key set in :data:`GEMINI_KEY_ENV_VARS`, or None."""
    for name in GEMINI_KEY_ENV_VARS:
        value = os.environ.get(name)
        if value:
            return value
    return None
