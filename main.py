#!/usr/bin/env python3
"""
    Simple script to pull data
"""

# Boilerplate
__author__ = "Ben"
__version__ = "1.0"
__license__ = "MIT"

import subprocess
import sys

def main() -> int:
    return subprocess.call([sys.executable, "./database/mgr.py", "pull"])

if __name__ == "__main__":
    raise SystemExit(main())