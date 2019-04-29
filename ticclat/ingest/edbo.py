import os.path
import pandas as pd

from ..dbutils import session_scope
from ..tokenize import terms_documents_matrix_ticcl_frequency
from ..sacoreutils import add_corpus_core

import glob


def ingest(session, base_dir='', data_dir='EDBO', **kwargs):
    in_dir = os.path.join(base_dir, data_dir)
    # TODO: decide how to deal with Xs in file names.
    # We are currently storing pub_year as int (which has some advantages),
    # how should we incorporate these files?
    # For now, we are ignoring them.
    in_files = glob.glob(os.path.join(in_dir, '*1'+'[0-9]' * 3+'.clean'))

    corpus_matrix, v = terms_documents_matrix_ticcl_frequency(in_files)

    document_metadata = pd.DataFrame()
    document_metadata['title'] = [os.path.splitext(os.path.basename(f))[0]
                                  for f in in_files]
    document_metadata['language'] = 'nl'
    document_metadata['pub_year'] = [int(os.path.basename(f).rsplit('.', 2)[1])
                                     for f in in_files]
    # More metadata?

    with session_scope(session) as s:
        add_corpus_core(s, corpus_matrix, v, 'Early Dutch Books Online',
                        document_metadata, **kwargs)
