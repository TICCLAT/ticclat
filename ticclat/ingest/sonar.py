import os.path
import pandas as pd

from ..dbutils import session_scope
from ..tokenize import terms_documents_matrix_ticcl_frequency
from ..sacoreutils import add_corpus_core

import glob

def ingest(session, base_dir='',
           sonar_dir='SONAR500', **kwargs):
    in_dir = os.path.join(base_dir, sonar_dir)
    in_files = glob.glob(os.path.join(in_dir, '*.wordfreqlist.clean.tsv.bz2'))

    corpus_matrix, vectorizer = terms_documents_matrix_ticcl_frequency(in_files)

    document_metadata = pd.DataFrame()
    document_metadata['title'] = [os.path.basename(f).split('.', 1)[0] for f in in_files]
    document_metadata['language'] = 'nl'
    # More metadata?

    with session_scope(session) as s:
        add_corpus_core(s, corpus_matrix, vectorizer, 'SoNaR-500', document_metadata, **kwargs)
