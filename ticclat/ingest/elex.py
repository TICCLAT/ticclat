"""e-Lex linked lexicon ingestion."""

import os.path
import pandas as pd
from ..dbutils import add_lexicon_with_links, session_scope


def ingest(session_maker, base_dir='',
           elex_lemma_file='elex/e-Lex-1.1.lemma_wordform.utf8.nonumbers.tsv', **kwargs):
    """
    Ingest e-Lex linked lexicon into TICCLAT database.

    This is a linked lexicon in which both words in the pair are considered to
    be "correct".
    """
    l_wf_pairs = pd.read_csv(os.path.join(base_dir, elex_lemma_file), sep='\t', header=None)
    l_wf_pairs.columns = ['lemma', 'variant']

    with session_scope(session_maker) as session:
        name = 'e-Lex-1.1.lemma_wordform.utf8.nonumbers'
        vocabulary = True
        from_column = 'lemma'
        from_correct = True
        to_column = 'variant'
        to_correct = True

        add_lexicon_with_links(session, name, vocabulary, l_wf_pairs,
                               from_column, to_column, from_correct, to_correct, **kwargs)
