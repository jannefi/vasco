from __future__ import annotations
import argparse, platform, sys


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env-check", action="store_true")
    args = p.parse_args(argv)
    if args.env_check:
        arch = platform.machine()
        print(
            f"Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} arch={arch}"
        )
        return 0
    p.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
