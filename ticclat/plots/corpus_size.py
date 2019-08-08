import pandas
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show
from bokeh import palettes

from ticclat import db


def corpus_size():
    query = """
SELECT SUM(word_count) AS sum_word_count,
       c.name AS name
FROM documents
         LEFT JOIN corpusId_x_documentId cIxdI on documents.document_id = cIxdI.document_id
         LEFT JOIN corpora c on cIxdI.corpus_id = c.corpus_id
GROUP BY c.corpus_id, c.name
ORDER BY sum_word_count DESC
"""
    connection = db.engine.connect()
    df = pandas.read_sql(query, connection)

    p = figure(
        title="Corpus size",
        sizing_mode='stretch_both',
        y_range=df['name'],
    )

    df['color'] = palettes.Category10[len(df)]

    p.hbar(
        y='name',
        right='sum_word_count',
        source=ColumnDataSource(df),
        color='color',
        height=1,
        fill_alpha=0.9,
        line_width=0,
        muted_alpha=0,
    )

    return p


if __name__ == '__main__':
    p = corpus_size()
    show(p)
