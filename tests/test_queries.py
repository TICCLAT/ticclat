import pytest
import os

import pandas as pd

from ticclat.tokenize import terms_documents_matrix_word_lists
from ticclat.dbutils import add_ticcl_variants
from ticclat.sacoreutils import add_corpus_core
from ticclat.utils import read_ticcl_variants_file

from ticclat.flask_app.queries import get_ticcl_variants

from .helpers import nltk_tokenize

from . import data_dir


@pytest.mark.datafiles(os.path.join(data_dir(), 'test_corpus.txt'))
@pytest.mark.datafiles(os.path.join(data_dir(), 'ticcl_variants.txt'))
def test_new_wordforms_with_count_zero(dbsession, datafiles):
    """If the ticcl variants contain a word that is not in the corpus (this
    shouldn't occur in practice), the frequency of the wordform in the
    corpus should be zero (and not be returned as link by
    ``get_ticcl_variants()``)
    """
    path = str(datafiles)
    texts_file = os.path.join(path, 'test_corpus.txt')
    variants_file = os.path.join(path, 'ticcl_variants.txt')

    texts_iterator = nltk_tokenize(texts_file)
    corpus_name = 'test corpus'

    corpus_m, v = terms_documents_matrix_word_lists(texts_iterator)

    add_corpus_core(dbsession, corpus_m, v, corpus_name, pd.DataFrame())

    variants = read_ticcl_variants_file(variants_file)
    lexicon = add_ticcl_variants(dbsession, 'ticcl variants test corpus', variants)

    result = get_ticcl_variants(dbsession, 'wf1', 1, lexicon.lexicon_id)

    print(result)

    assert result['correct']

    for word in result['links']:
        assert word['wordform'] != 'ws1'
