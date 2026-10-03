import assert from "node:assert/strict";
import { mock, test } from "node:test";
const forwarded = [];
mock.module("@cloudflare/containers", {
  namedExports: { Container: class {}, getContainer: () => ({ fetch(request) {
    forwarded.push(request);
    return new Response("container response");
  } }) },
});
const { default: worker } = await import("../worker/index.ts");
const env = { SPICY_REGS_R2_URL: "https://data.example.org", MCP_GUIDE_URL: "https://example.org/mcp/" };

for (const method of ["GET", "HEAD"]) {
  test(`${method} root redirects without requiring data or starting a container`, async () => {
    const before = forwarded.length;
    const response = await worker.fetch(new Request("https://mcp.example.org/", { method }), { ...env, SPICY_REGS_R2_URL: "" });
    assert.equal(response.status, 302);
    assert.equal(response.headers.get("Location"), env.MCP_GUIDE_URL);
    assert.equal(forwarded.length, before);
  });
}
test("unset guide preserves upstream landing page", async () => {
  const request = new Request("https://mcp.example.org/");
  const response = await worker.fetch(request, { ...env, MCP_GUIDE_URL: "" });
  assert.equal(await response.text(), "container response");
  assert.equal(forwarded.at(-1), request);
});
for (const path of ["/mcp", "/mcp/", "/mcp/session", "/"]) {
  test(`POST ${path} preserves the original request`, async () => {
    const request = new Request(`https://mcp.example.org${path}`, { method: "POST", body: '{"method":"initialize"}' });
    const response = await worker.fetch(request, env);
    assert.equal(response.status, 200);
    assert.equal(forwarded.at(-1), request);
    assert.equal(request.bodyUsed, false);
  });
}
test("protocol requests still fail clearly when data is unavailable", async () => {
  const response = await worker.fetch(new Request("https://mcp.example.org/mcp"), { ...env, SPICY_REGS_R2_URL: "" });
  assert.equal(response.status, 503);
});
test("unknown routes are not redirected", async () => {
  const response = await worker.fetch(new Request("https://mcp.example.org/nope"), env);
  assert.equal(response.status, 404);
});
