import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import datetime
import pandas as pd
import psycopg2

import db_config

app = dash.Dash(__name__)
conn = psycopg2.connect(user="faunam",
                        password="stupidpassword97!",
                        host="tone-db.ccg3nx1k7it5.us-west-2.rds.amazonaws.com",
                        port=5432,
                        database="postgres")

# https://help.plot.ly/database-connectors/query-from-plotly/

app.layout = html.Div(children=[
    html.Div(children='''
        We are graphing
    '''),

    dcc.Dropdown(
        id='entity-dropdown',
        options=[
            {'label': 'Donald Trump', 'value': 'donald trump'},
            {'label': 'Jeff Bezos', 'value': 'jeff bezos'},
            {'label': 'Starbucks', 'value': 'starbucks'}
        ],
        value='donald trump'
    ),
    html.Div(id='graph-options', children=[
        dcc.Checklist(id="media-legend",
                      options=[
                          {'label': 'News', 'value': 'news'},
                          {'label': 'Twitter', 'value': 'twitter'},
                      ],
                      value=['twitter']
                      ),
        dcc.DatePickerRange(id="date-range",
                            month_format='MMM Do, YY',  # ?? check datepicker plotly page if issues
                            end_date_placeholder_text='MM/DD/YY',
                            start_date=datetime(2015, 3, 21)
                            )

    ]),
    html.Div(id="graph")

])

# pd.read_sql_query() - use to transform sql into dataframe
# sql = "select count(*) from table;"
# dat = pd.read_sql_query(sql, conn)
# set conn = None at end


@app.callback(
    Output(component_id='graph', component_property='children'),
    [Input(component_id='entity-dropdown', component_property='value'),
     Input(component_id='media-legend', component_property='value'),
     Input(component_id='date-range', component_property='start_date'),
     Input(component_id='date-range', component_property='end_date')])
def update_value(entity, media_types, start_date, end_date):
    data = []
    for media in media_types:
        sql_str = "select tone, date from full_sample where entity='{}' and media='{}' limit 10".format(
            entity, media)  # start_date, end_date #and date<{} and date>{}
        media_df = pd.read_sql_query(sql_str, conn)
        data.append({
            "x": media_df.date,
            "y": media_df.tone,
            "type": "line",
            "name": media+"-"+entity
        })
    title_string = "Sentiment towards " + \
        " ".join([word.capitalize() for word in entity.split(" ")])

    return dcc.Graph(
        id='example-graph',
        figure={
            'data': data,
            'layout': {
                'title': title_string
            }
        }
    )


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=8080)
