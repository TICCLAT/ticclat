import os.path
import pandas as pd
from ..dbutils import add_lexicon_with_links, session_scope


def ingest(session, base_dir='',
           elex_words_file='elex/e-Lex-1.1.uniq.utf8.txt',
           elex_lemma_file='elex/e-Lex-1.1.lemma_wordform.utf8.nonumbers.tsv', **kwargs):
    l_wf_pairs = pd.read_csv(os.path.join(base_dir, elex_lemma_file), sep='\t', header=None)
    l_wf_pairs.columns = ['lemma', 'variant']

    with session_scope(session) as s:
        name = 'e-Lex-1.1.lemma_wordform.utf8.nonumbers'
        vocabulary = True
        from_column = 'lemma'
        from_correct = True
        to_column = 'variant'
        to_correct = True

        add_lexicon_with_links(s, name, vocabulary, l_wf_pairs, from_column, to_column, from_correct, to_correct, **kwargs)
