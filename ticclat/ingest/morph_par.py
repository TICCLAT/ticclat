import re
import copy
import os.path
import pandas as pd

from ..dbutils import session_scope, add_morphological_paradigms

import glob

INPUT = 'morph/CombilexTypolistINThistlex.TICCLATingest.DeriveParadigms265.tsv'

def ingest(session, base_dir='', in_file=INPUT, **kwargs):
    with session_scope(session) as s:
        add_morphological_paradigms(s, os.path.join(base_dir, in_file))
