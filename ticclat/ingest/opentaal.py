import os.path
import pandas as pd
from ..dbutils import add_lexicon, session_scope


def ingest(session, base_dir='',
           opentaal_file='OpenTaal/OpenTaal-210G-BasisEnFlexies.txt', **kwargs):
    wfs = pd.read_csv(os.path.join(base_dir, opentaal_file), header=None)
    wfs.columns = ['wordform']

    with session_scope(session) as s:
        name = 'OpenTaal-210G-BasisEnFlexies'
        vocabulary = True
        add_lexicon(s, name, vocabulary, wfs, **kwargs)
