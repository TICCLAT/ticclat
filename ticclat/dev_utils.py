"""Utilities used while developing TICCLAT"""

from ticclat.ticclat_schema import Lexicon


def delete_lexicon(session, lexicon_id):
    """Delete a lexicon from the database."""
    lexicon = session.query(Lexicon).get(lexicon_id)

    # delete relevant related objects
    lexicon.lexicon_wordform_links = []
    lexicon.lexicon_wordforms = []

    # delete lexicon
    session.delete(lexicon)
