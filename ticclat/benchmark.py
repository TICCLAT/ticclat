"""Functionality for benchmarking the ticclat database.
"""
import logging

import numpy as np
import pandas as pd

from faker import Faker

from ticclat.tokenize import terms_documents_matrix_word_lists
from ticclat.sacoreutils import add_corpus_core
from ticclat.dbutils import add_lexicon, add_lexicon_with_links

LOGGER = logging.getLogger(__name__)


def random_corpus(num_documents, num_tokens_min, num_tokens_max, vocabulary):
    """Generator that yields random bags of words (documents)."""
    fake = Faker()
    for _ in range(num_documents):
        num_tokens = np.random.randint(num_tokens_min, num_tokens_max + 1)

        yield fake.words(nb=num_tokens, ext_word_list=vocabulary, unique=False)


def corpus_metadata(num_documents, language, year_min, year_max):
    """Return a mock corpus metadata dataframe."""
    metadata = pd.DataFrame()
    metadata['language'] = [language for i in range(num_documents)]
    metadata['pub_year'] = [np.random.randint(year_min, year_max + 1)
                            for i in range(num_documents)]

    return metadata


def generate_corpora(num_corpora, num_documents_min, num_documents_max,
                     language, year_min, year_max, num_tokens_min,
                     num_tokens_max, vocabulary):
    """Generator that yields randomized corpus term-documents matrices."""
    for _ in range(num_corpora):
        num_documents = np.random.randint(num_documents_min,
                                          num_documents_max + 1)

        metadata = corpus_metadata(num_documents, language, year_min, year_max + 1)

        word_lists = random_corpus(num_documents, num_tokens_min,
                                   num_tokens_max + 1, vocabulary)
        corpus, vocabulary = terms_documents_matrix_word_lists(word_lists)

        yield corpus, vocabulary, metadata


def ingest_corpora(session, num_corpora, num_documents_min, num_documents_max,
                   language, year_min, year_max, num_tokens_min,
                   num_tokens_max, vocabulary):
    """Run multiple (random built) corpus ingestions."""
    corpus_data = generate_corpora(num_corpora, num_documents_min, num_documents_max,
                                   language, year_min, year_max, num_tokens_min,
                                   num_tokens_max, vocabulary)
    for i, (corpus, vocabulary_i, metadata) in enumerate(corpus_data):
        name = f'Corpus {i}'
        LOGGER.info('Generating %s', name)
        add_corpus_core(session, corpus_matrix=corpus, vectorizer=vocabulary_i,
                        corpus_name=name, document_metadata=metadata)


def generate_lexica(num_lexica, num_wf_min, num_wf_max, vocabulary):
    """Generator that yields randomized lexicon wordform lists."""
    fake = Faker()
    for _ in range(num_lexica):
        num_wf = np.random.randint(num_wf_min, num_wf_max)

        wfs = pd.DataFrame()
        wfs['wordform'] = fake.words(nb=num_wf, ext_word_list=vocabulary,
                                     unique=True)

        yield wfs


def ingest_lexica(session, num_lexica, num_wf_min, num_wf_max, vocabulary):
    """Run multiple (random built) lexicon ingestions."""
    lexica = generate_lexica(num_lexica, num_wf_min, num_wf_max + 1, vocabulary)
    for i, wfs in enumerate(lexica):
        name = f'Lexicon {i}'
        LOGGER.info('Generating %s', name)
        add_lexicon(session, lexicon_name=name, vocabulary=True, wfs=wfs)


def generate_linked_lexica(num_lexica, num_wf_min, num_wf_max, vocabulary):
    """Generator that yields randomized linked lexicon dataframes."""
    fake = Faker()
    for _ in range(num_lexica):
        num_wf = np.random.randint(num_wf_min, num_wf_max)
        if num_wf % 2 != 0:
            num_wf += 1

        words = fake.words(nb=num_wf, ext_word_list=vocabulary, unique=True)

        half = int(num_wf / 2)

        wfs = pd.DataFrame()
        wfs['from'] = words[:half]
        wfs['to'] = words[half:]
        yield wfs


def ingest_linked_lexica(session, num_lexica, num_wf_min, num_wf_max,
                         vocabulary):
    """Run multiple (random built) linked lexicon ingestions."""
    lexica = generate_linked_lexica(num_lexica, num_wf_min, num_wf_max + 1,
                                    vocabulary)

    for i, wfs in enumerate(lexica):
        name = f'Linked lexicon {i}'

        is_vocabulary = np.random.rand() > .5
        from_correct = is_vocabulary
        print(name, 'is_vocabulary:', is_vocabulary)

        LOGGER.info('Generating %s (is vocabulary: %s)', name, is_vocabulary)

        add_lexicon_with_links(session, lexicon_name=name,
                               vocabulary=is_vocabulary, wfs=wfs,
                               from_column='from', to_column='to',
                               from_correct=from_correct, to_correct=True)
