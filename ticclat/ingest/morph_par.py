import logging
import os.path

from ..ticclat_schema import MorphologicalParadigm
from ..dbutils import session_scope, add_morphological_paradigms, empty_table


logger = logging.getLogger(__name__)

INPUT = 'morph/CombilexTypolistINThistlex.TICCLATingest.DeriveParadigms267.TYPOLIST6.tsv'


def ingest(session, base_dir='', in_file=INPUT, **kwargs):
    with session_scope(session) as s:
        empty_table(s, MorphologicalParadigm)

        add_morphological_paradigms(s, os.path.join(base_dir, in_file))
