"""Functionality for benchmarking the ticclat database.
"""
import logging

import numpy as np
import pandas as pd

from faker import Faker

from ticclat.tokenize import terms_documents_matrix_word_lists
from ticclat.sacoreutils import add_corpus_core
from ticclat.dbutils import add_lexicon, add_lexicon_with_links

logger = logging.getLogger(__name__)


def random_corpus(num_documents, num_tokens_min, num_tokens_max, vocabulary):
    fake = Faker()
    for i in range(num_documents):
        num_tokens = np.random.randint(num_tokens_min, num_tokens_max + 1)

        yield fake.words(nb=num_tokens, ext_word_list=vocabulary, unique=False)


def corpus_metadata(num_documents, language, year_min, year_max):
    md = pd.DataFrame()
    md['language'] = [language for i in range(num_documents)]
    md['pub_year'] = [np.random.randint(year_min, year_max + 1)
                      for i in range(num_documents)]

    return md


def generate_corpora(num_corpora, num_documents_min, num_documents_max,
                     language, year_min, year_max, num_tokens_min,
                     num_tokens_max, vocabulary):
    for i in range(num_corpora):
        num_documents = np.random.randint(num_documents_min,
                                          num_documents_max + 1)

        md = corpus_metadata(num_documents, language, year_min, year_max + 1)

        word_lists = random_corpus(num_documents, num_tokens_min,
                                   num_tokens_max + 1, vocabulary)
        corpus, v = terms_documents_matrix_word_lists(word_lists)

        yield corpus, v, md


def ingest_corpora(session, num_corpora, num_documents_min, num_documents_max,
                   language, year_min, year_max, num_tokens_min,
                   num_tokens_max, vocabulary):
    ca = generate_corpora(num_corpora, num_documents_min, num_documents_max,
                          language, year_min, year_max, num_tokens_min,
                          num_tokens_max, vocabulary)
    for i, (corpus, v, md) in enumerate(ca):
        name = f'Corpus {i}'
        logger.info(f'Generating {name}')
        add_corpus_core(session, corpus_matrix=corpus, vectorizer=v,
                        corpus_name=name, document_metadata=md)


def generate_lexica(num_lexica, num_wf_min, num_wf_max, vocabulary):
    fake = Faker()
    for i in range(num_lexica):
        num_wf = np.random.randint(num_wf_min, num_wf_max)

        wfs = pd.DataFrame()
        wfs['wordform'] = fake.words(nb=num_wf, ext_word_list=vocabulary,
                                     unique=True)

        yield wfs


def ingest_lexica(session, num_lexica, num_wf_min, num_wf_max, vocabulary):
    lexica = generate_lexica(num_lexica, num_wf_min, num_wf_max + 1, vocabulary)
    for i, wfs in enumerate(lexica):
        name = f'Lexicon {i}'
        logger.info(f'Generating {name}')
        add_lexicon(session, lexicon_name=name, vocabulary=True, wfs=wfs)


def generate_linked_lexica(num_lexica, num_wf_min, num_wf_max, vocabulary):
    fake = Faker()
    for i in range(num_lexica):
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
    lexica = generate_linked_lexica(num_lexica, num_wf_min, num_wf_max + 1,
                                    vocabulary)

    for i, wfs in enumerate(lexica):
        name = f'Linked lexicon {i}'

        is_vocabulary = np.random.rand() > .5
        from_correct = is_vocabulary
        print(name, 'is_vocabulary:', is_vocabulary)

        logger.info(f'Generating {name} (is vocabulary: {is_vocabulary})')

        add_lexicon_with_links(session, lexicon_name=name,
                               vocabulary=is_vocabulary, wfs=wfs,
                               from_column='from', to_column='to',
                               from_correct=from_correct, to_correct=True)
