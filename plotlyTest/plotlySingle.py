import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/solar.csv")

app.layout = dbc.Container([
    html.H1("Interactive Graphs Dashboard", className='mb-2', style={'textAlign': 'center'}),
    dcc.Tabs([
        dcc.Tab(label='Matplotlib Bar', children=[
            dcc.Graph(id='matplotlib-bar-graph')
        ]),
        dcc.Tab(label='Plotly Pie', children=[
            dcc.Graph(id='plotly-pie-graph')
        ]),
        dcc.Tab(label='Plotly Scatter', children=[
            dcc.Graph(id='plotly-scatter-graph')
        ]),
    ])
])

@app.callback(
    dash.dependencies.Output('matplotlib-bar-graph', 'figure'),
    [dash.dependencies.Input('matplotlib-bar-graph', 'id')]
)
def update_matplotlib_bar_graph(_):
    fig = px.bar(df, x='State', y='Number of Solar Plants', title='Bar Graph of Solar Plants')
    return fig

@app.callback(
    dash.dependencies.Output('plotly-pie-graph', 'figure'),
    [dash.dependencies.Input('plotly-pie-graph', 'id')]
)
def update_plotly_pie_graph(_):
    fig = px.pie(df, values='Number of Solar Plants', names='State', title='Distribution of Solar Plants')
    return fig

@app.callback(
    dash.dependencies.Output('plotly-scatter-graph', 'figure'),
    [dash.dependencies.Input('plotly-scatter-graph', 'id')]
)
def update_plotly_scatter_graph(_):
    # Correcting the column name from 'MWatts' to 'Installed Capacity (MW)'
    fig = px.scatter(df, x='State', y='Installed Capacity (MW)', color='State', size='Installed Capacity (MW)', title='State Wise Installed Capacity')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, port=8002)
