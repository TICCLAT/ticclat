import numpy
import pandas
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure, show
from bokeh import palettes

from ticclat.flask_app.db import database


def lexicon_size():
    query = """
SELECT SUBSTR(lexica.lexicon_name, 1, 20) AS name, COUNT(wordform_id) / 1e5 AS word_count
FROM lexical_source_wordform
LEFT JOIN lexica on lexical_source_wordform.lexicon_id = lexica.lexicon_id
GROUP BY lexical_source_wordform.lexicon_id
ORDER BY word_count DESC
"""
    connection = database.session.connection()
    df = pandas.read_sql(query, connection)

    p = figure(
        title="Lexicon size",
        sizing_mode='stretch_both',
        y_range=df['name'],
        tools=['hover', 'pan', 'wheel_zoom', 'save', 'reset']
    )

    clipped_df_len = numpy.clip(len(df), 3, 10)

    df['color'] = palettes.Category10[clipped_df_len][0:len(df)]

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

    p.xaxis.axis_label = 'Number of words [× 10^5]'
    p.yaxis.axis_label = 'Lexicon'
    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [
        ("Lexicon", "@name"),
        ("Number of words [× 10^5]", "@word_count"),
    ]
    hover.mode = 'mouse'
    return p


if __name__ == '__main__':
    p = lexicon_size()
    show(p)
