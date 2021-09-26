"""
Compile a metrics repository.

This will:

    1. Build graph of nodes.
    2. Compute the schema of source nodes.
    3. Infer the schema of downstream nodes.

"""

import os
from pathlib import Path

import yaml
from sqlmodel import SQLModel, create_engine

from datajunction.models import Source
from datajunction.utils import load_config


async def run(repository: Path) -> None:
    """
    Compile the metrics repository.
    """
    config = load_config(repository)

    engine = create_engine(config["index"])
    SQLModel.metadata.create_all(engine)

    # compute schema of source nodes
    sources_directory = repository / "sources"
    for path in sources_directory.glob("**/*.yaml"):
        # XXX handle '.' in name?
        name = str(path.relative_to(sources_directory).with_suffix("")).replace(
            os.path.sep, "."
        )
        with open(path, encoding="utf-8") as input_:
            source_data = yaml.load(input_, Loader=yaml.SafeLoader)
        source_data["name"] = name

        from pprint import pprint

        pprint(source_data)
        source = Source(**source_data)
