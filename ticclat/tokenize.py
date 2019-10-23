"""Generators that produce term-frequency vectors of documents in a corpus.

A document in ticclat is a term-frequency vector (collections.Counter). This
module contains generators that return term-frequency vectors for certain types
of input data.
"""
import bz2

from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import CountVectorizer


def ticcl_frequency(in_files, max_word_length=255):
    """
    Generate word-frequency pairs from TICCL frequency files.

    For each file in `in_files`, open it and yield a dictionary with
    frequencies (value) for each word (key).
    """
    for freq_file in in_files:
        word_freqs = {}
        if freq_file.endswith('bz2'):
            file_open = bz2.open(freq_file, 'rt')
        else:
            file_open = open(freq_file)

        with file_open as file_handle:
            for line in file_handle:
                # Sometimes a word contains a space, so we split only on tab.
                word, freq = line.split('\t')

                # The corpus may contain wordforms that are too long
                if len(word) <= max_word_length:
                    word_freqs[word] = int(freq)
        yield word_freqs


def do_nothing(list_of_words):
    """Return the argument unchanged."""
    return list_of_words


def terms_documents_matrix_word_lists(word_lists):
    """Returns a terms document matrix and related objects of a corpus

    Inputs:
        word_lists: iterator over lists of words
    Returns:
        corpus: a sparse terms documents matrix
        vocabulary: the vecorizer object containing the vocabulary (i.e., all word forms
                    in the corpus)
    """
    vocabulary = CountVectorizer(tokenizer=do_nothing, lowercase=False)
    corpus = vocabulary.fit_transform(word_lists)

    return corpus, vocabulary


def terms_documents_matrix_ticcl_frequency(in_files):
    """Returns a terms document matrix and related objects of a corpus

    Inputs:
        in_files: list of ticcl frequency files (one per document in the
            corpus)
    Returns:
        corpus: a sparse terms documents matrix
        vocabulary: the vecorizer object containing the vocabulary (i.e., all word forms
                    in the corpus)
    """
    vocabulary = DictVectorizer()
    corpus = vocabulary.fit_transform(ticcl_frequency(in_files))

    return corpus, vocabulary
