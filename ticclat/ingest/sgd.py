import os.path
import glob

import pandas as pd

from ..dbutils import session_scope
from ..tokenize import terms_documents_matrix_ticcl_frequency
from ..sacoreutils import add_corpus_core


def ingest(session_maker, base_dir='', sgd_dir='SGD', **kwargs):
    in_dir = os.path.join(base_dir, sgd_dir)
    in_files = glob.glob(os.path.join(in_dir, '*.clean'))

    corpus_matrix, vocabulary = terms_documents_matrix_ticcl_frequency(in_files)

    document_metadata = pd.DataFrame()
    document_metadata['title'] = [os.path.splitext(os.path.basename(f))[0]
                                  for f in in_files]
    document_metadata['language'] = 'nl'
    document_metadata['pub_year'] = [int(os.path.basename(f).rsplit('.', 2)[1])
                                     for f in in_files]
    # More metadata?

    with session_scope(session_maker) as session:
        add_corpus_core(session, corpus_matrix, vocabulary, 'Staten Generaal Digitaal',
                        document_metadata, **kwargs)
