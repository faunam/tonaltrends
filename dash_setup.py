import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import datetime
import pandas as pd
import psycopg2
import textwrap
import json
from textwrap import dedent as d


app = dash.Dash()
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
        options=[  # automate this maybe?
            {'label': 'Donald Trump', 'value': 'donald trump'},
            {'label': 'Jeff Bezos', 'value': 'jeff bezos'},
            {'label': 'Facebook', 'value': 'facebook'},
            {'label': 'Bernie Sanders', 'value': 'bernie sanders'},
            {'label': 'Amazon', 'value': 'amazon'}
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
                            month_format='MM/DD/YY',  # ?? check datepicker plotly page if issues
                            end_date_placeholder_text='MM/DD/YY',
                            start_date=datetime(2015, 2, 19).strftime(
                                "%m-%d-%Y"),
                            end_date=datetime(2015, 2, 20).strftime(
                                "%m-%d-%Y")
                            )
    ]),
    html.Div(id="graph", children=[
             dcc.Graph(
                 id='ex-graph',
                 figure={
                     'data': [],
                     'layout': {
                         'title': "hehe",
                         'clickmode': 'event+select'
                     }
                 }
             )
             ]),
    html.Div([
        dcc.Markdown(d("""
            **Instance info** -  Mouse over values in the graph.
        """)),
        html.Pre(id='hover-data')
    ])

])

# pd.read_sql_query() - use to transform sql into dataframe
# sql = "select count(*) from table;"
# dat = pd.read_sql_query(sql, conn)
# set conn = None at end


def wrap_helper(text, width):
    return "\n".join(textwrap.wrap(text, width=width))


@app.callback(
    Output('hover-data', 'children'),
    [Input('ex-graph', 'hoverData')])
def display_hover_data(hoverData):
    if hoverData is None:
        return "None"
    return wrap_helper(hoverData["points"][0]['text'], 100)


@app.callback(
    Output(component_id='graph', component_property='children'),
    [Input(component_id='entity-dropdown', component_property='value'),
     Input(component_id='media-legend', component_property='value'),
     Input(component_id='date-range', component_property='start_date'),
     Input(component_id='date-range', component_property='end_date')])
def update_value(entity, media_types, start_date, end_date):
    data = []
    for media in media_types:
        print(media)
#        sql_str = "select tone, date, text from full_sample where entity='{}' and media='{}'".format(entity, media)
        sql_str = "select tone, date, text from full_sample where entity='{}' and media='{}' and date between '{}' and '{}'".format(
            entity, media, start_date, end_date)
        media_df = pd.read_sql_query(sql_str, conn)
        media_df["tone"] = pd.to_numeric(media_df["tone"])
        # print(media_df.info(verbose=True))
        #media_df["date"] = pd.to_datetime(media_df["date"])
        media_df = media_df.groupby(["date"]).agg(
            {"tone": "mean", "text": "first", "date": "first"})
        print(media_df.info(verbose=True))
        fig = go.Figure(data=[go.Scatter(x=media_df.date, y=media_df.tone)])

        data.append({
            "x": media_df.date,
            "y": media_df.tone,
            "text": media_df.text,
            "type": "line",
            "name": media,
            "mode": 'lines+markers',
            'marker': {'size': 5}
            #            "hovertemplate": wrap_helper('<b>{}</b><br>%{text}'.format(
            #                    media), 30),
        })
    title_string = "Sentiment towards " + \
        " ".join([word.capitalize() for word in entity.split(" ")])

    return dcc.Graph(
        id='ex-graph',
        figure={
            'data': data,
            'layout': {
                'title': title_string,
                'clickmode': 'event+select'
            }
        }
    )


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=8080)
