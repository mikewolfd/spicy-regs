# Bill source wheels

`uv sync --frozen` installs these through the default `bill-sources` group.
The optional `bill-sources` extra describes the same feature for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.3.0`: built from SpicyDocs commit
  `8e485fe052c794cf18b041debe970c813812c05b`.
- `rulespec_artifacts-1.0.12`: copied unchanged from that SpicyDocs checkout.
- `uv.lock` records both wheel SHA-256 values. Replace the wheel and refresh the
  lock together, then run the receiver tests against the installed wheel.
