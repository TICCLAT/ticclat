from ..dbutils import session_scope, create_wf_frequencies_table


# TODO: this should not be ingest, there's no ingestion, an aggregation table is created from existing data.

def ingest(session_maker, **kwargs):
    with session_scope(session_maker) as session:
        create_wf_frequencies_table(session)
