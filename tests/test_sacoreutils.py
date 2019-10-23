import os
import pytest

import pandas as pd

from ticclat.ticclat_schema import Wordform, Corpus
from ticclat.tokenize import terms_documents_matrix_word_lists

from ticclat.sacoreutils import add_corpus_core

from .helpers import nltk_tokenize

from . import data_dir


@pytest.mark.datafiles(os.path.join(data_dir(), 'test_corpus.txt'))
def test_add_corpus_core_nltk(dbsession, datafiles):
    texts_file = os.path.join(str(datafiles), 'test_corpus.txt')

    expected_wordforms = ['wf1', 'wf2', 'wf3', 'wf4', 'wf5']
    texts_iterator = nltk_tokenize(texts_file)
    corpus_name = 'test corpus'

    corpus_m, v = terms_documents_matrix_word_lists(texts_iterator)

    # TODO: add test that adds metadata; replace pd.DataFrame() with mock dataframe containing metadata fields.
    # Currently, the database contains no data in any of the following columns: persistent_id, encoding, author,
    # editor, publisher, publishing_location, text_type, region, other_languages, spelling and parent_document.
    add_corpus_core(dbsession, corpus_m, v, corpus_name, pd.DataFrame())

    # wordforms
    wrdfrms = dbsession.query(Wordform).order_by(Wordform.wordform).all()

    assert len(wrdfrms) == 5
    assert [w.wordform for w in wrdfrms] == expected_wordforms

    # corpus
    corpora = dbsession.query(Corpus).all()

    assert len(corpora) == 1

    corpus = corpora[0]

    assert corpus.name == corpus_name

    # documents
    assert len(corpus.corpus_documents) == 3
    for d in corpus.corpus_documents:
        assert d.word_count == 3

    # text attestations
    for d, num_tas in zip(corpus.corpus_documents, [3, 3, 2]):
        assert len(d.document_wordforms) == num_tas
        for ta in d.document_wordforms:
            if d.document_id == 3 and ta.ta_wordform.wordform == 'wf1':
                assert ta.frequency == 2
            else:
                assert ta.frequency == 1
