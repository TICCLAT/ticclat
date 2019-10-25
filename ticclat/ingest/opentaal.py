"""OpenTaal lexicon ingestion."""

import os.path
import pandas as pd
from ..dbutils import add_lexicon, session_scope


def ingest(session_maker, base_dir='',
           opentaal_file='OpenTaal/OpenTaal-210G-BasisEnFlexies.txt', **kwargs):
    """Ingest OpenTaal lexicon into TICCLAT database."""
    wfs = pd.read_csv(os.path.join(base_dir, opentaal_file), header=None)
    wfs.columns = ['wordform']

    with session_scope(session_maker) as session:
        # name = 'OpenTaal-210G-BasisEnFlexies'
        name = 'Open Taal modern Dutch word list'
        vocabulary = True
        add_lexicon(session, name, vocabulary, wfs, **kwargs)
