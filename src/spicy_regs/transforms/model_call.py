"""Where this host's Gemini key comes from. The call itself is spicy-docs'.

Until 0.22.0 this module also held the adapter joining
``spicy_docs.extraction.gemini.GeminiClient`` to the ``ModelCall`` seam the
model-backed interpretation modules take. That adapter now lives upstream as
``spicy_docs.interpretation.gemini_call``, beside both halves it joins, and
the copy here is deleted rather than kept in step: 0.22.0 gave ``ModelCall`` a
``response_schema`` argument that every generator passes, and a local adapter
whose ``call(*, model, prompt)`` did not take it raised ``TypeError`` on every
call. One seam that changes with its own callers is the point of the move.

What stays here is the one thing that is this application's and not the
library's: **which environment variable holds the key**. A hosting application
supplies the key; spicy-docs never reads the environment for one.

The adapter's own behaviour — the request body, the schema on the request, the
candidate envelope, the refusal that does not echo model prose, the token
counts — is proved in spicy-docs' ``tests/test_interpretation_gemini_call.py``,
beside the code that decides it. The tests this module used to carry for it
were deleted with it; a second copy of a claim can only drift from the first.
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
