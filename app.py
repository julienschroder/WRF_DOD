import dash, flask, os
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import pickle

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', 'secret')
app = dash.Dash(name = __name__, server = server)
app.config.supress_callback_exceptions = True
dic = pickle.load( open( './data/WRF_extract_GFDL_1970-2100_multiloc_dod.p', "rb" ) )
df_greely_historical = pd.read_csv('./data/truth.csv',index_col=0)
df_greely_historical.index = pd.to_datetime( df_greely_historical.index )


temp = ('C1 : 0 to -25','C2 : -25.1 to -50','C3 : colder than -50')
values = (0 , -25 , -40 )

app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
app.layout = html.Div([
   html.Div(
        [
            html.H1(
                'WRF Temperature Exploration for DOD project',
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
                 options=[{'label': 'Consecutive days : {}'.format(i), 'value': i} for i in range(1,10)],
                 value=2
             ),
         ],className='three columns'),
         html.Div([
             dcc.Dropdown(
                 id='temperature',
                 options=[{'label': 'Temperature below : {} celsius'.format(i), 'value': i} for i in range(0,-40, -5)],
                 value=0
             )
         ],className='three columns'),
          html.Div([
              dcc.Dropdown(
                id='location',
                options=[{'label': 'Location: {}'.format(i), 'value': i} for i in dic.keys() ],
                value='Greely'
              )
          ],className='three columns'),

     ]),
     html.Div([
         dcc.Graph(id='indicator-graphic'),
     ],className='eleven columns')
 ],className='ten columns offset-by-one')
@app.callback(
    dash.dependencies.Output('indicator-graphic', 'figure'),
    [dash.dependencies.Input('nb_days', 'value'),
     dash.dependencies.Input('temperature', 'value'),
     dash.dependencies.Input('location', 'value')])

def update_graph(nb_days, temperature, location):
    def rolling_count_serie(serie , temperature , nb_days):
        ct = 0
        ls = []
        for i in serie :
            if i <= temperature and ct < nb_days :
                ct +=1
            elif ct == nb_days :
                ct = 1
            else :
                ct = 0

            ls.append(ct)
        return ls

    df = dic[ location ].copy()

    df.index = pd.to_datetime( df.index )
    df['count'] = rolling_count_serie(df['max'], temperature , int(nb_days))

    dff = df[ df['count'] == int(nb_days) ]
    dff = dff.groupby( dff.index.year ).count()


    df_greely_historical['count'] = rolling_count_serie(df_greely_historical['max'], temperature , int(nb_days))
    df_hist = df_greely_historical[ df_greely_historical['count'] == int(nb_days) ]
    df_hist = df_hist.groupby( df_hist.index.year ).count()
    df_hist = df_hist.loc[1970:]

    if location == 'Greely' :
        return {
           'data': [go.Bar(
               x = dff.index,
               y = dff['count'],
               name = 'WRF modeled'
           ),
                    go.Scatter(
               x = dff.index,
               y = df_hist['count'],
               mode = 'markers',
               name = 'Greely historical'
           )],
           'layout': go.Layout(
               xaxis=dict(
                   title =  'Years',
                   range = [1969,2101],
                   ),
               yaxis={
                   'title': 'Number of occurences',
                   },
               margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
               hovermode='closest'
           )
        }
    else :
        return {
           'data': [go.Bar(
               x=dff.index,
               y=dff['count'],
               name = 'WRF modeled'
           )],
           'layout': go.Layout(
               xaxis={
                   'title': 'Years',
                   'autorange':True,
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
