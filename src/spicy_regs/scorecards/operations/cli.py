"""Operate scorecards through maintained commands and immutable prepared inputs."""

import argparse
import sys
from importlib import import_module

COMMANDS = {
    "inspect-adapters": "inspect",
    "inventory": "inventory",
    "qualifications": "qualifications",
    "record-publication": "record",
    "prepare": "prepare",
    "prepare-analysis": "analysis",
    "publish": "publish",
    "readback": "readback",
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=COMMANDS, help="Choose a phase; readback never publishes")
    argv = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(argv[:1])
    remaining = argv[1:]
    import_module("spicy_regs.scorecards.operations." + COMMANDS[args.command]).main(remaining)


if __name__ == "__main__":
    main()
