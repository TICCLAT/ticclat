import os.path
import pandas as pd

from ..dbutils import session_scope
from ..tokenize import terms_documents_matrix_ticcl_frequency
from ..sacoreutils import add_corpus_core

import glob


def ingest(session, base_dir='', data_dir='DBNL', **kwargs):
    in_dir = os.path.join(base_dir, data_dir)
    # TODO: uniformize year ingestion.
    # For this batch of data, Martin used "exact" year range file-names,
    # e.g. 1720+2 stands for 1720, 1721 and/or 1722. Before he used X's to
    # indicate ranges for specific digits, which we didn't deal with yet.
    # For this batch, we thus use the more exact ranges.
    # We now also use only year_from / year_to, instead of pub_year. When
    # there is only one year for a file, we simply put year_from == year_to.
    in_files = glob.glob(os.path.join(in_dir, '*.clean'))

    corpus_matrix, v = terms_documents_matrix_ticcl_frequency(in_files)

    document_metadata = pd.DataFrame()
    document_metadata['title'] = [os.path.splitext(os.path.basename(f))[0]
                                  for f in in_files]
    document_metadata['language'] = ['lim' if 'LIM' in f else 'nl'
                                     for f in in_files]

    years = [os.path.basename(f).rsplit('.', 2)[1].split('_')[0] for f in in_files]
    year_from = [int(y.split('+')[0]) for y in years]
    year_to = [int(y.split('+')[0]) + int(y.split('+')[1]) if len(y.split('+')) > 1 else int(y.split('+')[0]) for y in years]

    document_metadata['year_from'] = year_from
    document_metadata['year_to'] = year_to
    # More metadata?

    with session_scope(session) as s:
        add_corpus_core(s, corpus_matrix, v, 'Databank Nederlandse Literatuur (DBNL)',
                        document_metadata, **kwargs)
