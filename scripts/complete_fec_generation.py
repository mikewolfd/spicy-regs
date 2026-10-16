"""Verify retained FEC observations using completed proofs and bounded checks."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from spicy_regs.fec_release_verification import verify_release  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('admission', 'retained-admission', 'checkpoint', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('admission-digest', 'retained-digest'):
        parser.add_argument('--' + name, required=True)
    result = verify_release(**vars(parser.parse_args()))
    print(json.dumps({key: result[key] for key in ('generationPin', 'releaseBasis', 'seconds', 'qualifiedForRelease')}))


if __name__ == '__main__':
    main()
