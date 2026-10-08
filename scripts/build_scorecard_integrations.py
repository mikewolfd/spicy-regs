"""Compatibility entry point for maintained scorecard operations."""

from spicy_regs.scorecards.operations.inventory import (
    work_priorities as work_priorities,
    recovery_findings as recovery_findings,
    implementation_findings as implementation_findings,
    survey_findings as survey_findings,
    pinned_document as pinned_document,
    nested as nested,
    read_receipt as read_receipt,
    generate as generate,
    current_api_inventory as current_api_inventory,
    main as main,
    )


if __name__ == "__main__":
    main()
