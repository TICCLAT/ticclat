from ..dbutils import session_scope, create_wf_frequencies_table


def ingest(session_maker, **kwargs):
    with session_scope(session_maker) as session:
        create_wf_frequencies_table(session)
