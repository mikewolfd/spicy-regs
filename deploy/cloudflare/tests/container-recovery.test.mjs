import assert from "node:assert/strict";
import { mock, test } from "node:test";
import { readFileSync } from "node:fs";
import "./python-text.mjs";

const startup = readFileSync(new URL("../start_reader.py", import.meta.url), "utf8");

class ContainerBoundary {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }
  async fetch(request) {
    this.ctx.forwarded.push(request);
    return this.ctx.response;
  }
  async startAndWaitForPorts(options) {
    this.ctx.starts.push(options);
    if (this.ctx.startError) throw this.ctx.startError;
    this.ctx.container.running = true;
  }
}
mock.module("@cloudflare/containers", {
  namedExports: { Container: ContainerBoundary, getContainer: () => {} },
});
const { McpContainer } = await import("../worker/index.ts");

test("SDK entrypoint embeds the same launch owner used by the image", () => {
  assert.deepEqual(new McpContainer({}, {}).entrypoint, ["python", "-c", startup]);
});

function scenario({ status = 500, running = true, probeError, startError } = {}) {
  const ctx = {
    forwarded: [], probes: [], resets: [], starts: [], startError,
    response: new Response("Application response must remain unread", { status }),
    container: {
      running,
      getTcpPort(port) {
        assert.equal(port, 8080);
        return { async fetch(url, options) {
          ctx.probes.push({ url, options });
          if (probeError) throw probeError;
          return new Response(null, { status: 200 });
        } };
      },
    },
    abort(message) {
      ctx.resets.push(message);
      throw new Error("object reset");
    },
  };
  const request = new Request("https://mcp.example/mcp", {
    method: "POST", body: '{"method":"tools/call"}',
  });
  return { ctx, request, instance: new McpContainer(ctx, {}) };
}

test("native port contradiction resets the object without replaying the POST", async () => {
  const { ctx, request, instance } = scenario({
    probeError: new Error("The container is not running, consider calling start()"),
  });
  await assert.rejects(instance.fetch(request), /object reset/);
  assert.deepEqual(ctx.forwarded, [request]);
  assert.equal(ctx.resets.length, 1);
  assert.equal(ctx.probes[0].options.method, "HEAD");
  assert.ok(ctx.probes[0].options.signal instanceof AbortSignal);
  assert.equal(ctx.response.bodyUsed, false);
});

test("application errors remain unchanged when the native port answers", async () => {
  const { ctx, request, instance } = scenario();
  assert.equal(await instance.fetch(request), ctx.response);
  assert.equal(ctx.response.bodyUsed, false);
  assert.equal(ctx.resets.length, 0);
});

for (const probeError of [new Error("Network connection lost"), new DOMException("timeout", "TimeoutError")]) {
  test(`other native failures do not reset: ${probeError.name} ${probeError.message}`, async () => {
    const { ctx, request, instance } = scenario({ probeError });
    assert.equal(await instance.fetch(request), ctx.response);
    assert.equal(ctx.response.bodyUsed, false);
    assert.equal(ctx.resets.length, 0);
    assert.deepEqual(ctx.forwarded, [request]);
  });
}

for (const options of [{ status: 200 }, { status: 503 }]) {
  test(`non-500 responses skip port recovery: ${JSON.stringify(options)}`, async () => {
    const { ctx, request, instance } = scenario(options);
    assert.equal(await instance.fetch(request), ctx.response);
    assert.equal(ctx.probes.length, 0);
    assert.equal(ctx.resets.length, 0);
    assert.equal(ctx.response.bodyUsed, false);
  });
}

test("refused startup returns 503 without forwarding or replaying the POST", async () => {
  const { ctx, request, instance } = scenario({
    running: false, startError: new Error("private startup diagnostic"),
  });
  const response = await instance.fetch(request);
  assert.equal(response.status, 503);
  assert.equal(await response.text(), "Reader release is not ready.");
  assert.deepEqual(ctx.forwarded, []);
  assert.equal(request.bodyUsed, false);
  assert.equal(ctx.starts.length, 1);
  assert.equal(ctx.starts[0].cancellationOptions.instanceGetTimeoutMS, 60_000);
  assert.equal(ctx.starts[0].cancellationOptions.portReadyTimeoutMS, 60_000);
  assert.equal(ctx.starts[0].cancellationOptions.abort, request.signal);
});

test("successful replacement starts once then forwards the untouched request", async () => {
  const { ctx, request, instance } = scenario({ running: false, status: 200 });
  assert.equal(await instance.fetch(request), ctx.response);
  assert.equal(ctx.starts.length, 1);
  assert.deepEqual(ctx.forwarded, [request]);
  assert.equal(request.bodyUsed, false);
  assert.equal(await instance.fetch(request), ctx.response);
  assert.equal(ctx.starts.length, 1);
});
