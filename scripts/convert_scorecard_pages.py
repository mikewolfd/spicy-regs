"""Convert pinned legacy page captures into the existing lossless evidence format.

This migration reads only explicitly pinned private files and permits only the
SpicyDocs extraction data classes. The normal integration path reads the resulting
typed JSON; it never loads pickle or re-runs an extraction model.
"""

import argparse
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import pickle

from spicy_docs.extraction import model

from spicy_regs.scorecards.extraction_replay import PageObservationReplay, page_bytes, read_page
from spicy_regs.scorecards.retained import pinned_bytes

ALLOWED = {
    name: getattr(model, name)
    for name in (
        "Box",
        "Raster",
        "TextBlock",
        "ProcessingIssue",
        "TableCell",
        "Observation",
        "PageContent",
        "TableObservation",
        "PageResult",
    )
}


class PageUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module == "spicy_docs.extraction.model" and name in ALLOWED:
            return ALLOWED[name]
        raise pickle.UnpicklingError("Legacy observation requests a type outside the extraction data model")


def convert(manifest_path, manifest_sha256, root, output):
    manifest = json.loads(pinned_bytes(manifest_path, manifest_sha256, max_bytes=1024**2))
    root = root.resolve()
    if output.exists():
        raise ValueError("Use a fresh observation migration directory")
    selected = []
    for record in manifest["pages"]:
        path = (root / record["file"]).resolve()
        if Path(record["file"]).is_absolute() or not path.is_relative_to(root):
            raise ValueError("Legacy page path leaves the selected private corpus")
        page = PageUnpickler(BytesIO(pinned_bytes(path, record["sha256"]))).load()
        if not isinstance(page, model.PageResult):
            raise ValueError("Legacy observation is not a SpicyDocs page")
        selected.append((record, page))
    PageObservationReplay([page for _, page in selected])
    output.mkdir(parents=True)
    converted = []
    for ordinal, (record, page) in enumerate(selected):
        body = page_bytes(page)
        if read_page(body) != page:
            raise ValueError("Migrated page differs from its original typed observation")
        name = f"{ordinal:04}.json"
        (output / name).write_bytes(body)
        converted.append(
            dict(
                file=name,
                sha256=sha256(body).hexdigest(),
                legacy_file=record["file"],
                legacy_sha256=record["sha256"],
                page=page.metadata["page"],
                source_sha256=page.metadata["source_sha256"],
                byte_size=len(body),
            )
        )
    receipt = dict(
        format_version="scorecard-page-migration/1",
        complete=True,
        input_manifest_sha256=manifest_sha256,
        pages=converted,
        raw_values_and_page_shapes_equal=True,
        model_requests=0,
    )
    (output / "migration.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    result = convert(args.manifest, args.manifest_sha256, args.root, args.output)
    print(json.dumps(dict(complete=result["complete"], pages=len(result["pages"]), model_requests=0)))


if __name__ == "__main__":
    main()
