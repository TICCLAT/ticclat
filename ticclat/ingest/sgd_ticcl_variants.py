"""Staten Generaal Digitaal TICCL variants ingestion."""

import logging
import glob
from pathlib import Path

import pandas as pd

from ticclat.utils import read_ticcl_variants_file
from ticclat.dbutils import add_ticcl_variants, session_scope

LOGGER = logging.getLogger(__name__)


def ingest(session_maker, base_dir='/', sgd_ticcl_variants_dir='SGD_ticcl_variants', **kwargs):
    """Ingest Staten Generaal Digitaal TICCL variants into TICCLAT database."""
    # TODO: now all correction data is read into memory before ingestion. For
    # the total amount of data this probably is not feasible, so this needs
    # to be fixed later.
    in_dir = Path(base_dir) / sgd_ticcl_variants_dir
    expr = Path(in_dir) / '*'
    in_files = glob.glob(str(expr))

    LOGGER.debug('Reading data from: %s', in_dir)
    LOGGER.debug('Ingesting files: %s', ' - '.join(in_files))

    dfs = []

    for in_file in in_files:
        df = read_ticcl_variants_file(in_file)
        dfs.append(df)
    data = pd.concat(dfs)

    with session_scope(session_maker) as session:
        name = 'SGD ticcl correction candidates'
        add_ticcl_variants(session, name, data, **kwargs)
