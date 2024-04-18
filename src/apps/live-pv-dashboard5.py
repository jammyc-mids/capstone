import dash
from dash import dash_table
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
import pgAPIpecan as pgapi

selected_house_id = 2818
timestamp = pd.to_datetime("11am on 10/11/2018")
end_timestamp = pd.to_datetime("11am on 10/12/2018")

external_stylesheets = [dbc.themes.DARKLY]

app = dash.Dash(__name__)

dark_theme_styles = {
    'background-color': '#343a40',
    'color': '#6c757d',
}

state_list_unique = pgapi.getStates()
state_select = [{"label": item, "value": item} for item in state_list_unique]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)
app.css.append_css({"external_url": "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"})

app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    html.Div(id="page-content"),html.Div(className='wrapper', children=[
        html.Div(className='content-wrapper', style={'margin-right': '5%', 'margin-left':'5%','margin-bottom':'5%', 'margin-top':'3%'}, children=[
            html.H1(id='house-title', style={'textAlign': 'left', 'padding-left':'10px','padding-bottom':'10px'}),
            html.Div(className='content', children=[
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-6', children=[
                            html.H3('Select Date'),
                            dcc.DatePickerRange(
                                id='date-time-range',
                                start_date=timestamp,
                                end_date=end_timestamp,
                                display_format='YYYY-MM-DD'
                            )
                        ]),
                        html.Div(className='col-md-6', children=[
                            html.H3('Select Location'),
                            dcc.Dropdown(
                                id='tb-state-dropdown',
                                placeholder='Select State',
                                options=state_select,
                                value='',
                                style={'color': '#6c757d', 'padding-bottom':'5px'}
                            ),
                            dcc.Dropdown(
                                id='tb-county-dropdown',
                                placeholder='Select County',
                                disabled=True,
                                value='',
                                style={'color': '#6c757d', 'padding-bottom':'5px'}
                            ),
                            dcc.Dropdown(
                                id='tb-household-dropdown',
                                placeholder='Select Household',
                                multi=True,
                                disabled=True,
                                value='',
                                style={'color': '#6c757d', 'padding-bottom':'5px'}
                            )
                        ])
                    ])
                ]),
                html.Div([
                    html.Button('Start', id='start-button', n_clicks=0, style={'margin': '5px'}),
                    html.Button('Pause', id='pause-button', n_clicks=0, style={'margin': '5px'}),
                ]),
                html.Div(id='top-graph-container', style={'height': '50vh', 'padding': '10px'}, children=[
                    #dcc.Graph(id='irradiance-graph', style={'height': '100%', 'backgroundColor': '#343a40'})
                    dcc.Graph(id='irradiance-graph', style={'height': '100%'}, config={'displayModeBar': False}, figure={
                        'layout': {
                            'plot_bgcolor': '#343a40',
                                'paper_bgcolor': '#343a40'
                        }
                    })
                ]),
                html.Div(style={'height': '50vh', 'padding': '10px'}, children=[
                    #dcc.Graph(id='net-load-pv-graph', style={'height': '100%', 'backgroundColor': '#343a40'})
                    dcc.Graph(id='net-load-pv-graph', style={'height': '100%'}, config={'displayModeBar': False}, figure={
                        'layout': {
                            'plot_bgcolor': '#343a40',
                                'paper_bgcolor': '#343a40'
                        }
                    })
                ]),
                dcc.Interval(id='interval-component', interval=1000, n_intervals=0),
                dcc.Store(id='selected_house_id', data=selected_house_id),
                dcc.Store(id='timestamp', data=timestamp),
                dcc.Store(id='end_timestamp', data=end_timestamp)



            ])
        ])
    ])
], fluid=True)







historical_time_irr = []
historical_time_load = []
historical_net_load = []
historical_pv = []
historical_irr = []
updating = False
paused = False

@app.callback(
    Output('pause-button', 'children'),
    [Input('pause-button', 'n_clicks')]
)
def toggle_pause_resume(n_clicks):
    global paused
    if n_clicks % 2 == 1:
        paused = True
        return 'Resume'
    else:
        paused = False
        return 'Pause'

@app.callback(
    Output('tb-county-dropdown', 'disabled'),
    [Input('tb-state-dropdown', 'value')]
)
def enable_county_dropdown(selected_state):
    return selected_state is None

@app.callback(
    Output('tb-household-dropdown', 'disabled'),
    [Input('tb-state-dropdown', 'value'),
     Input('tb-county-dropdown', 'value')]
)
def enable_house_dropdown(selected_state, selected_county):
    return selected_state is None and selected_county is None

@app.callback(
    Output('tb-county-dropdown', 'options'),
    [Input('tb-state-dropdown', 'value')]
)
def update_county_dropdown(selected_state):
    if selected_state:
        county_list_unique = pgapi.getCounties(selected_state)
        county_select = [{"label": item, "value": item} for item in county_list_unique]
        return county_select
    else:
        return []

@app.callback(
    Output('tb-household-dropdown', 'options'),
    [Input('tb-county-dropdown', 'value'),
     Input('tb-state-dropdown', 'value')]
)
def update_household_dropdown(selected_county, selected_state):
    if selected_state is None or selected_county is None:
        return []
    else:
        household_ids = pgapi.getHouseIds(selected_state, selected_county)
        household_select = [{"label": str(house_id), "value": house_id} for house_id in household_ids]
        return household_select

@app.callback(
    Output('house-title', 'children'),
    [Input('selected_house_id', 'data')]
)
def update_house_title(selected_house_id):
    if isinstance(selected_house_id, list):
      return f"Solar Estimation for House {selected_house_id[0]}"
    f"Solar Estimation for House {selected_house_id}"



@app.callback(
    [Output('irradiance-graph', 'figure'),
     Output('net-load-pv-graph', 'figure')],
    [Input('interval-component', 'n_intervals'),
     Input('start-button', 'n_clicks'),
     Input('pause-button', 'n_clicks')],
    [State('selected_house_id', 'data')]
)
def update_graphs(n_intervals, start_clicks, pause_clicks, house_id):
    global historical_time_irr, historical_irr, historical_time_load, historical_net_load, historical_pv, timestamp, paused
    if start_clicks % 2 == 0:  # Even clicks or initial load
        return go.Figure(), go.Figure()

    if timestamp >= end_timestamp or paused:
        raise dash.exceptions.PreventUpdate

    if not historical_time_load:
        historical_time_load.append(timestamp)
        historical_net_load.append(0)
        historical_pv.append(0)
    else:
        current_time = historical_time_load[-1] + pd.Timedelta(minutes=15)
        historical_time_load.append(current_time)

    try:
        irradiance = pgapi.getIrradiance(house_id, timestamp)
        historical_irr.append(irradiance)
    except:
        historical_irr.append(None)
    historical_time_irr.append(timestamp)

    try:
        net_load = pgapi.getNetLoad(house_id, timestamp)
        historical_net_load.append(net_load)
    except:
        historical_net_load.append(None)

    try:
        pv_load = pgapi.getPVLoad(house_id, timestamp)
        historical_pv.append(pv_load)
    except:
        historical_pv.append(None)

    visible_irr = historical_irr
    visible_time = historical_time_irr

    trace_irr = go.Scatter(
        x=visible_time,
        y=visible_irr,
        mode='lines+markers',
        name='Irradiance',
        line=dict(color='green')
    )

    net_load_trace = go.Scatter(
        x=visible_time,
        y=historical_net_load,
        mode='lines+markers',
        name='Net Load'
    )

    pv_trace = go.Scatter(
        x=visible_time,
        y=historical_pv,
        mode='lines+markers',
        name='PV Value'
    )

    layout = go.Layout(
        xaxis=dict(title=''),
        yaxis=dict(title='kWh'),
        plot_bgcolor='#343a40',
        paper_bgcolor='#343a40',
        font=dict(color='#6c757d'),
        showlegend=True
    )

    timestamp += pd.Timedelta(minutes=15)

    return {'data': [trace_irr], 'layout': layout}, {'data': [net_load_trace, pv_trace], 'layout': layout}






@app.callback(
    Output('start-button', 'n_clicks'),
    [Input('start-button', 'n_clicks')],
    [State('interval-component', 'n_intervals')]
)
def reset_button_click_counter(n_clicks, n_intervals):
    global timestamp, historical_time_irr, historical_time_load, historical_net_load, historical_pv, historical_irr
    print('here1', n_clicks)
    if n_clicks % 2 == 0:
        reset_data()
        timestamp = pd.to_datetime("11am on 10/11/2018")
        return 0
    else:
        return n_clicks


def reset_data():
    global historical_time_irr, historical_time_load, historical_net_load, historical_pv, historical_irr
    historical_time_irr = []
    historical_time_load = []
    historical_net_load = []
    historical_pv = []
    historical_irr = []

@app.callback(
    Output('selected_house_id', 'data'),
    [Input('tb-household-dropdown', 'value')]
)
def update_selected_house_id(selected_house_id):
    return selected_house_id if selected_house_id else 2818

@app.callback(
    Output('date-time-range', 'start_date'),
    Output('date-time-range', 'end_date'),
    [Input('tb-state-dropdown', 'value')]
)
def update_date_range_default(state_values):
    if state_values == 'NY':
        start_date, end_date = pgapi.getDefaultDatesNY()
    elif state_values == 'TX':
        start_date, end_date = '2018-01-03', '2018-01-04'
    else:
        start_date, end_date = pgapi.getDefaultDates()
    return start_date, end_date

@app.callback(
    Output('date-time-range', 'disabled'),
    [Input('tb-state-dropdown', 'value'),
     Input('tb-county-dropdown', 'value'),
     Input('tb-household-dropdown', 'value')]
)
def update_date_range_disabled(state_value, county_value, household_value):
    if state_value and county_value and household_value:
        return False
    else:
        return True


@app.callback(
    [Output('timestamp', 'data'),
     Output('end_timestamp', 'data')],
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')]
)
def update_timestamp(start_date, end_date):
    global timestamp, end_timestamp
    timestamp = pd.to_datetime(start_date)
    end_timestamp = pd.to_datetime(end_date)
    print('udpated timestamps', timestamp, end_timestamp)
    return timestamp, end_timestamp



if __name__ == '__main__':
    app.run_server(host="0.0.0.0", port=8052, debug=True)

