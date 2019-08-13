import logging
import glob

import pandas as pd

from ticclat.dbutils import add_lexicon_with_links, session_scope

logger = logging.getLogger(__name__)


def ingest(session, base_dir='/data/SGD_ticcl_variants', **kwargs):
    # TODO: now all correction data is read into memory before ingestion. For
    # the total amount of data this probably is not feasible, so this needs
    # to be fixed later.
    in_files = glob.glob('{}/*'.format(base_dir))

    dfs = []

    for in_file in in_files:
        df = pd.read_csv(in_file, sep='#', header=None, engine='python')
        df.columns = ['ocr_variant', 'corpus_frequency',
                      'correction_candidate', '?1', 'ld', '?2', 'anahash']
        dfs.append(df)
    data = pd.concat(dfs)

    with session_scope(session) as s:
        name = 'SDG ticcl correction candidates'
        vocabulary = False
        from_column = 'ocr_variant'
        from_correct = False
        to_column = 'correction_candidate'
        to_correct = True
        preprocess_wfs = False
        to_add = ['ld']

        add_lexicon_with_links(s,
                               name,
                               vocabulary,
                               data,
                               from_column,
                               to_column,
                               from_correct,
                               to_correct,
                               preprocess_wfs=preprocess_wfs,
                               to_add=to_add)
