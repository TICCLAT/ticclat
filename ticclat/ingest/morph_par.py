"""Morphological paradigms ingestion."""

import os.path

from ..ticclat_schema import MorphologicalParadigm
from ..dbutils import session_scope, add_morphological_paradigms, empty_table

# ! In this version of the data, the first line contains noise and is removed
# ! before ingesting the data.
# ! When changing the input file, please check if the first line of the file
# ! still needs to be removed.
# INPUT = 'morph/CombilexTypolistINThistlex.TICCLATingest.DeriveParadigms265.tsv'
INPUT = 'morph/CombinationGigantMolexCombilexTypolist6INThistlex.TICCLATingest.NEWSPLITSnoplus.DeriveParadigms306.delfirstlines.KopStaartCodes.tsv'


def ingest(session_maker, base_dir='', morph_par_file=INPUT, **kwargs):
    """
    Ingest morphological paradigms into TICCLAT database.

    Removes any existing data from the morphological_paradigms database table.
    """
    with session_scope(session_maker) as session:
        empty_table(session, MorphologicalParadigm)

        add_morphological_paradigms(session, os.path.join(base_dir, morph_par_file))
