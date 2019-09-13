import logging

from ticclat.ticclat_schema import Lexicon, WordformLinkSource

logger = logging.getLogger(__name__)


def delete_lexicon(session, lexicon_id):
    lexicon = session.query(Lexicon).get(lexicon_id)

    # delete relevant related objects
    lexicon.lexicon_wordform_links = []
    lexicon.lexicon_wordforms = []

    # delete lexicon
    session.delete(lexicon)
