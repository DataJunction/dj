import argparse
import os
from datajunction_server.api.main import graphql_schema


def save_graphql_schema(output_dir: str):
    schema_sdl = graphql_schema.as_str()
    with open(os.path.join(output_dir, "schema.graphql"), "w") as f:
        f.write(schema_sdl)
    print("Schema generated successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a file containing the OpenAPI spec for a DJ server",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="directory",
        required=True,
    )
    args, _ = parser.parse_known_args()
    save_graphql_schema(output_dir=args.directory)
