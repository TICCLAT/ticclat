import numpy
import pandas
from bokeh import models
from bokeh.models import HoverTool
from bokeh.plotting import figure, show
from bokeh import palettes

from ticclat.flask_app.db import database


def corpus_size():
    query = """
SELECT SUM(word_count) / 1e8 AS sum_word_count,
       c.name AS name
FROM documents
         LEFT JOIN corpusId_x_documentId cIxdI on documents.document_id = cIxdI.document_id
         LEFT JOIN corpora c on cIxdI.corpus_id = c.corpus_id
GROUP BY c.corpus_id, c.name
ORDER BY sum_word_count DESC
"""
    connection = database.session.connection()
    df = pandas.read_sql(query, connection)

    p = figure(
        title="Corpus size",
        sizing_mode='stretch_both',
        y_range=df['name'],
        active_scroll='wheel_zoom',
        tools=['hover', 'pan', 'wheel_zoom', 'save', 'reset']
    )

    clipped_df_len = numpy.clip(len(df), 3, 10)

    df['color'] = palettes.Category10[clipped_df_len][0:len(df)]

    p.hbar(
        y='name',
        right='sum_word_count',
        source=models.ColumnDataSource(df),
        color='color',
        height=1,
        fill_alpha=0.9,
        line_width=0,
        muted_alpha=0,
    )

    p.xaxis.axis_label = 'Number of words (tokens) [× 10^8]'
    p.yaxis.axis_label = 'Corpus'

    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [
        ("Corpus", "@name"),
        ("Number of words;", "@word_count × 10^8")
    ]
    hover.mode = 'mouse'

    return p


if __name__ == '__main__':
    p = corpus_size()
    show(p)
