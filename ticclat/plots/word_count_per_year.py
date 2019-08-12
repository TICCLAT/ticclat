import pandas
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure, show
from bokeh import palettes

from ticclat import db


def word_count_per_year():
    query = """
SELECT SUM(word_count) AS sum_word_count,
       CASE
           WHEN pub_year IS NOT NULL THEN pub_year
           ELSE ROUND((year_from + year_to) / 2)
       END AS year,
       c.name AS name
FROM documents
         LEFT JOIN corpusId_x_documentId cIxdI on documents.document_id = cIxdI.document_id
         LEFT JOIN corpora c on cIxdI.corpus_id = c.corpus_id
GROUP BY year, c.corpus_id, c.name
ORDER BY year
"""
    connection = db.engine.connect()
    df = pandas.read_sql(query, connection)

    p = figure(
        title="Word (token) count per year",
        sizing_mode='stretch_both',
        y_axis_type="log",
        y_range=[1e3, 1e8],
        tools=['hover', 'pan', 'wheel_zoom', 'save', 'reset'],
        active_scroll = 'wheel_zoom',
    )

    corpus_names = df['name'].unique()

    palette = palettes.Category10[10]

    for i, corpus_name in enumerate(corpus_names):
        corpus_data = df[df['name'] == corpus_name]
        corpus_data['color'] = palette[i]
        p.vbar(
            x='year',
            top='sum_word_count',
            bottom=1e3,
            source=ColumnDataSource(corpus_data),
            color='color',
            width=1,
            fill_alpha=0.9,
            line_width=0,
            muted_alpha=0,
            legend='name'
        )

    p.xaxis.axis_label = 'Year'
    p.yaxis.axis_label = 'Number of words (tokens)'

    p.legend.location = 'top_left'
    p.legend.click_policy = "mute"

    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [
        ("Corpus", "@name"),
        ("Color", "$color[swatch]:color"),
        ("Year", "@year"),
        ("Number of words (tokens)", "@sum_word_count"),
    ]
    hover.mode = 'mouse'
    return p


if __name__ == '__main__':
    p = word_count_per_year()
    show(p)
