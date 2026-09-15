# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.15.0`: built from SpicyDocs commit `da531c4`.
  SHA-256: `deb8d5cfbdc426effcfecc008b43d9b819c3014f8a5d6b87539a72d651b2df0f`.
- `rulespec_artifacts-1.0.12`: unchanged from the prior source-reader pin.
- `uv.lock` records both wheel SHA-256 values. Replace the wheel and refresh the
  lock together, then run receiver tests against the installed wheel.

PDF enrichment uses the narrow `pdf-pypdf` provider extra through `source-readers`.
