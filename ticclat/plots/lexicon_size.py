import pandas
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show
from bokeh import palettes

from ticclat import db


def lexicon_size():
    query = """
SELECT SUBSTR(lexica.lexicon_name, 1, 20) AS name, COUNT(wordform_id) AS word_count
FROM lexical_source_wordform
LEFT JOIN lexica on lexical_source_wordform.lexicon_id = lexica.lexicon_id
GROUP BY lexical_source_wordform.lexicon_id
ORDER BY word_count DESC
"""
    connection = db.engine.connect()
    df = pandas.read_sql(query, connection)

    p = figure(
        title="Lexicon size",
        sizing_mode='stretch_both',
        y_range=df['name'],
    )

    df['color'] = palettes.Category10[len(df)]

    p.hbar(
        y='name',
        right='word_count',
        source=ColumnDataSource(df),
        color='color',
        height=1,
        fill_alpha=0.9,
        line_width=0,
        muted_alpha=0,
    )

    return p


if __name__ == '__main__':
    p = lexicon_size()
    show(p)
