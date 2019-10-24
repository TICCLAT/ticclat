"""Groene Boekje lexica ingestion."""

import os.path
import pandas as pd
from ..dbutils import add_lexicon, session_scope


def ingest(session_maker, base_dir='',
           gb1914_file='GB/1914/GB1914.expanded.types.utf-8.lst',
           gb19952005_file='GB/1995-2005/GB95-05_002.csv.alltokens.utf8.nopunct.lst', **kwargs):
    """Ingest Groene Boekje lexica into TICCLAT database."""
    # 1914
    wfs = pd.read_csv(os.path.join(base_dir, gb1914_file), header=None)
    wfs.columns = ['wordform']

    with session_scope(session_maker) as session:
        name = 'GB1914.expanded.types.utf-8'
        vocabulary = True
        add_lexicon(session, name, vocabulary, wfs, **kwargs)

    # 1995/2005
    wfs = pd.read_csv(os.path.join(base_dir, gb19952005_file), header=None)
    wfs.columns = ['wordform']

    with session_scope(session_maker) as session:
        name = 'GB95-05_002.csv.alltokens.utf8.nopunct'
        vocabulary = True
        add_lexicon(session, name, vocabulary, wfs, **kwargs)
