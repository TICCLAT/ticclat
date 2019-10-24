import logging
import os.path

from ..ticclat_schema import MorphologicalParadigm
from ..dbutils import session_scope, add_morphological_paradigms, empty_table


logger = logging.getLogger(__name__)

# ! In this version of the data, the first line contains noise and is removed
# ! before ingesting the data.
# ! When changing the input file, please check if the first line of the file
# ! still needs to be removed.
# INPUT = 'morph/CombilexTypolistINThistlex.TICCLATingest.DeriveParadigms265.tsv'
INPUT = 'morph/CombinationGigantMolexCombilexTypolist6INThistlex.TICCLATingest.NEWSPLITSnoplus.DeriveParadigms306.delfirstlines.KopStaartCodes.tsv'


def ingest(session, base_dir='', in_file=INPUT, **kwargs):
    with session_scope(session) as s:
        empty_table(s, MorphologicalParadigm)

        add_morphological_paradigms(s, os.path.join(base_dir, in_file))
