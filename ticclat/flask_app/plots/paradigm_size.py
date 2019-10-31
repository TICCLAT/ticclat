import pandas
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure, show

from ticclat.flask_app.db import database


def paradigm_size(var='X'):
    if var not in ['X', 'Y', 'Z']:
        raise AttributeError('var should be X or Y or Z')

    query = f"""
SELECT {var}, COUNT(W) AS num_paradigms FROM morphological_paradigms
GROUP BY {var}
ORDER BY num_paradigms DESC, {var} ASC
"""
    connection = database.session.connection()
    df = pandas.read_sql(query, connection)
    df[var] = [str(x) for x in df[var]]
    p = figure(
        title=f"Paradigm size ({var})",
        sizing_mode='stretch_both',
        x_range=df[var],
        y_axis_type="log",
        y_range=[1e-1, 1e6],
        tools=['hover', 'pan', 'wheel_zoom', 'save', 'reset']
    )

    source = ColumnDataSource(df)

    # callback=CustomJS(args=dict(source=source), code="""console.log(cb_data.source.selected)""")

    # p.add_tools(TapTool(callback=callback))

    vbar = p.vbar(
        x=var,
        bottom=1e-10,
        top='num_paradigms',
        source=source,
        width=1,
        fill_alpha=0.9,
        line_width=0,
        muted_alpha=0,
    )

    p.xaxis.major_tick_line_color = None
    p.xaxis.minor_tick_line_color = None
    p.xaxis.major_label_text_font_size = '0pt'

    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [
        (var, f"@{var}"),
        ("Number paradigms", "@num_paradigms"),
    ]
    hover.mode = 'mouse'
    return p


if __name__ == '__main__':
    p = paradigm_size('Z')
    show(p)
