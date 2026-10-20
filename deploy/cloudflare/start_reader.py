"""Start the reader only when its selected receipt matches the installed code.

The Worker embeds these same bytes as its entrypoint override: during a default
Cloudflare rollout, the previous image can still start under the new Worker.
Keep this entrypoint compatible with the maintained APIs in that prior image.
"""

import os


def main():
    receipt_digest = os.environ.get("SPICY_REGS_FEC_RELEASE_SHA256", "")
    image_digest = os.environ.get("SPICY_REGS_CONSUMER_IMAGE_DIGEST", "")
    # Unqualified local/fork serving remains available only with both pins absent.
    if receipt_digest or image_digest:
        from spicy_regs.fec_release import load_receipt, runtime_consumer
        from spicy_regs.runtime_bounds import RequestBound

        try:
            with RequestBound(60, "reader startup deadline").scope():
                receipt = load_receipt(receipt_digest, os.environ["SPICY_REGS_R2_URL"])
                if receipt["consumer"] != runtime_consumer(image_digest):
                    raise ValueError("Installed reader does not match the selected receipt")
        except Exception as error:
            # Transport errors may contain private settings. Do not echo them.
            raise SystemExit(f"Reader release refused before startup ({type(error).__name__})") from None
    os.execvp(
        "uvicorn",
        [
            "uvicorn", "spicy_regs.mcp_server:build_app", "--factory",
            "--host", "0.0.0.0", "--port", os.environ.get("PORT", "8080"),
        ],
    )


if __name__ == "__main__":
    main()
