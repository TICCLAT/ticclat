import logging
import glob

import pandas as pd

from pathlib import Path

from ticclat.utils import read_ticcl_variants_file
from ticclat.dbutils import add_ticcl_variants, session_scope

logger = logging.getLogger(__name__)


def ingest(session, base_dir='/', data_dir='SGD_ticcl_variants', **kwargs):
    # TODO: now all correction data is read into memory before ingestion. For
    # the total amount of data this probably is not feasible, so this needs
    # to be fixed later.
    in_dir = Path(base_dir) / data_dir
    expr = Path(in_dir) / '*'
    in_files = glob.glob(str(expr))

    logger.debug('Reading data from: {}'.format(in_dir))
    logger.debug('Ingesting files: {}'.format(' - '.join(in_files)))

    dfs = []

    for in_file in in_files:
        df = read_ticcl_variants_file(in_file)
        dfs.append(df)
    data = pd.concat(dfs)

    with session_scope(session) as s:
        name = 'SDG ticcl correction candidates'
        add_ticcl_variants(s, name, data)
