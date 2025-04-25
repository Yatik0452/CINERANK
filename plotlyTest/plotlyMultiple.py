import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/solar.csv")

# App layout with URL routing
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Index layout: simple links to the different graphs
index_page = html.Div([
    html.H1("Interactive Graphs Dashboard", className='mb-2', style={'textAlign': 'center'}),
    dbc.Nav([
        dbc.NavLink("Bar Graph", href='/matplotlib-bar'),
        dbc.NavLink("Pie Chart", href='/plotly-pie'),
        dbc.NavLink("Scatter Plot", href='/plotly-scatter'),
    ], vertical=True, pills=True),
])

# Each graph's layout in a function
def matplotlib_bar():
    fig = px.bar(df, x='State', y='Number of Solar Plants', title='Bar Graph of Solar Plants')
    return dcc.Graph(figure=fig)

def plotly_pie():
    fig = px.pie(df, values='Number of Solar Plants', names='State', title='Distribution of Solar Plants')
    return dcc.Graph(figure=fig)

def plotly_scatter():
    fig = px.scatter(df, x='State', y='Installed Capacity (MW)', color='State', size='Installed Capacity (MW)', title='State Wise Installed Capacity')
    return dcc.Graph(figure=fig)

# Callback to update the page content based on the URL path
@app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/matplotlib-bar':
        return matplotlib_bar()
    elif pathname == '/plotly-pie':
        return plotly_pie()
    elif pathname == '/plotly-scatter':
        return plotly_scatter()
    else:
        return index_page

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, port=8002)
