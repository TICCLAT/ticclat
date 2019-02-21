from shutil import copytree

from ticclat.ticclat_schema import Wordform, Corpus
from ticclat.tokenize import nltk_tokenize

from ticclat.sacoreutils import add_corpus_core


def setup_data(tmpdir):
    datadir = tmpdir.join('data').strpath
    copytree('tests/data', datadir)


def test_add_corpus_core(dbsession, tmpdir):
    setup_data(tmpdir)
    texts_file = tmpdir.join('data/test_corpus.txt').strpath

    expected_wordforms = ['wf1', 'wf2', 'wf3', 'wf4', 'wf5']
    texts_iterator = nltk_tokenize(texts_file)
    corpus_name = 'test corpus'

    add_corpus_core(dbsession, texts_iterator, corpus_name)

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
