"""Compatibility entry point for maintained scorecard operations."""

from spicy_regs.scorecards.operations.record import (
    document as document,
    hosted_counts as hosted_counts,
    publication_proof as publication_proof,
    promote as promote,
    main as main,
    )


if __name__ == "__main__":
    main()
