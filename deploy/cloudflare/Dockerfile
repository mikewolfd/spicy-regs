# Container image for the Spicy Regs MCP server (deployed to Cloudflare
# Containers; also runs anywhere that serves an $PORT HTTP container).
#
# Runs the CANONICAL server (src/spicy_regs/mcp_server.py:build_app) directly —
# not the Vercel copy. Importing mcp_server pulls only duckdb + mcp (verified),
# never the ETL deps (boto3/polars/pyarrow), so we install the package with
# --no-deps and add just the runtime trio, keeping the image small and cold
# starts fast. This is why the container can be the single source of truth once
# Vercel is retired.
FROM python:3.12-slim

# duckdb needs no system libs; keep the image minimal. Unbuffered stdout so the
# platform captures logs live.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Runtime deps only, pinned like pyproject.toml (the <2 mcp bound is
# load-bearing — mcp 2.0 removed mcp.server.fastmcp). uvicorn serves the ASGI app.
# loguru: the publication index reader the server imports to resolve fork
# generations logs through it; without it every tool call failed (found by the
# 2026-09-25 local smoke before the first fork deploy). The duckdb floor is the
# package's own.
RUN pip install "mcp>=1.27.1,<2" "duckdb>=1.5.3,<2" "uvicorn[standard]>=0.30" "loguru>=0.7"

# Install the package code without its declared (ETL) dependencies. mcp_server
# imports only the local _icon module plus mcp/duckdb, so this is sufficient and
# keeps boto3/polars/etc out of the image.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .
# Fail the build, not the first request, when a module the server imports at
# request time needs a package this image does not install.
RUN python -c "import spicy_regs.mcp_server, spicy_regs.sources.publication, spicy_regs.local_data"

# The platform sends traffic to $PORT (default 8080). build_app() mounts the MCP
# endpoint at /mcp; --factory because build_app is a callable returning the app.
ENV PORT=8080
EXPOSE 8080
CMD ["sh", "-c", "uvicorn spicy_regs.mcp_server:build_app --factory --host 0.0.0.0 --port ${PORT}"]
