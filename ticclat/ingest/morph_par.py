import os.path

from ..dbutils import session_scope, add_morphological_paradigms

INPUT = 'morph/CombilexTypolistINThistlex.TICCLATingest.DeriveParadigms267.TYPOLIST6.tsv'


def ingest(session, base_dir='', in_file=INPUT, **kwargs):
    with session_scope(session) as s:
        add_morphological_paradigms(s, os.path.join(base_dir, in_file))
