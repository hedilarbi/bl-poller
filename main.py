import sys
import os

# Ensure project root is on the path so `import db` resolves to db.py shim.
sys.path.insert(0, os.path.dirname(__file__))

from poller_core.loop import run

if __name__ == "__main__":
    run()
