import dash, flask, os
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import urllib.request, json

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', 'secret')
app = dash.Dash(name = __name__, server = server)
app.config.supress_callback_exceptions = True

temp = ('C1 : 0 to -25','C2 : -25.1 to -50','C3 : colder than -50')
values = (0 , -25 , -40 )

app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
app.layout = html.Div([
   html.Div(
        [
            html.H1(
                'WRF Temperature Exploration for DOD project - Fairbanks location',
                className='eight columns',
            ),
            html.Img(
                src="https://www.snap.uaf.edu/sites/all/themes/snap_bootstrap/logo.png",
                className='one columns',
                style={
                    'height': '80',
                    'width': '225',
                    'float': 'right',
                    'position': 'relative',
                },
            ),
        ],
        className='row'
    ),
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='nb_days',
                options=[{'label': 'Consecutive days : {}'.format(i), 'value': i} for i in range(10)],
                value=2
            ),
        ],className='six columns'),
        html.Div([
            dcc.Dropdown(
                id='temperature',
                options=[{'label': 'Temperature below : {} celsius'.format(i), 'value': i} for i in range(0,-40, -5)],
                value=0
            )
        ],className='six columns')

    ]),
    html.Div([
        dcc.Graph(id='indicator-graphic'),
    ],className='eleven columns')
],className='ten columns offset-by-one')
@app.callback(
    dash.dependencies.Output('indicator-graphic', 'figure'),
    [dash.dependencies.Input('nb_days', 'value'),
     dash.dependencies.Input('temperature', 'value')])
def update_graph(nb_days,temperature):

    api_url = "http://localhost:3000/thresholds/temp?days_window={days}&max_temperature={temp}"
    # TODO: would need error handling
    with urllib.request.urlopen(api_url.format(days=nb_days, temp=temperature)) as url:
        dff = df.from_dict(json.loads(url.read().decode()))

    return {
        'data': [go.Bar(
            x=dff.index,
            y=dff['occurences']
        )],
        'layout': go.Layout(
            xaxis={
                'title': 'Years',
                },
            yaxis={
                'title': 'Number of occurences',
                },
            margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
            hovermode='closest'
        )
    }


if __name__ == '__main__':
    app.server.run()
