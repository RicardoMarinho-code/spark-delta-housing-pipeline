"""Entry point for the Azure Data Factory DatabricksSparkPython activity.

ADF calls this file with the step parameters (e.g. ["silver", "--table", "families",
"--run-id", "<pipeline RunId>"]); the project wheel is attached as an activity library.
"""

import sys

from housing_lakehouse.cli import main

if __name__ == "__main__":
    code = main(sys.argv[1:])
    if code:
        raise SystemExit(code)
