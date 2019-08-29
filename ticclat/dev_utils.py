import logging

from ticclat.ticclat_schema import Lexicon, WordformLinkSource

logger = logging.getLogger(__name__)


def delete_lexicon(session, lexicon_id):
    engine = session.get_bind()
    lexicon = session.query(Lexicon).get(lexicon_id)

    # delete relevant related objects
    lexicon.lexicon_wordforms = []
    lexicon.lexicon_wordform_links = []

    # delete lexicon
    d = Lexicon.__table__.delete().where(Lexicon.lexicon_id == lexicon_id)
    engine.execute(d)
