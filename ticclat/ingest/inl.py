import os.path
import pandas as pd

from ..dbutils import add_lexicon, session_scope


def ingest(session, base_dir='',
           inl_file='INL/INL_lexicon.csv', **kwargs):
    """
    This function ingests a CSV dump of the relevant part of the INL database.
    This CSV dump was created by first using the EE3.5_Dutch_IR.Lexicon.sql to
    create the INL database and then selecting the relevant data from that.
    The file notebooks/ingest/lexicon_to_ticclat.ipynb shows how this can be
    done (basically: dump the wfs DataFrame to csv).
    """
    wfs = pd.read_csv(os.path.join(base_dir, inl_file), keep_default_na=False)

    with session_scope(session) as s:
        name = 'INL_EE3-5_Dutch_IR'
        vocabulary = True
        add_lexicon(s, name, vocabulary, wfs)
