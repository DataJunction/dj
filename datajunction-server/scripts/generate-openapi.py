#!/usr/bin/env python3
# pylint: skip-file

import argparse
import json

from datajunction_server.api.main import app


def save_openapi_spec(f: str):
    spec = app.openapi()
    with open(f, "w") as outfile:
        outfile.write(json.dumps(spec, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a file containing the OpenAPI spec for a DJ server",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        dest="filename",
        required=True,
        metavar="FILE",
    )
    args = vars(parser.parse_args())
    save_openapi_spec(f=args["filename"])
