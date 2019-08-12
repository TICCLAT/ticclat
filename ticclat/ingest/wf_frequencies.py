from ..dbutils import session_scope, create_wf_frequencies_table


def ingest(session, **kwargs):
    with session_scope(session) as s:
        create_wf_frequencies_table(s)
