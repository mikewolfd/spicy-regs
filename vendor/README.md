# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.12.0`: built from SpicyDocs commit `e847f5c`.
  SHA-256: `a231f4975b9cb07e4ca0dd80dc6fb9c94db43bc61004d9384f62f3b0936b7ac6`.
- `rulespec_artifacts-1.0.12`: unchanged from the prior source-reader pin.
- `uv.lock` records both wheel SHA-256 values. Replace the wheel and refresh the
  lock together, then run receiver tests against the installed wheel.
