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

"""
app.layout = html.Div(style=dict(dark_theme_styles, **{'height': '100vh'}), children=[
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
    html.Div(className='col-md-6 text-right', children=[
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
]),





    html.Div([
        html.Button('Start', id='start-button', n_clicks=0, style={'margin': '5px'}),
        html.Button('Pause', id='pause-button', n_clicks=0, style={'margin': '5px'}),
    ]),
    html.Div(id='top-graph-container', style={'height': '50vh', 'padding': '10px'}, children=[
        html.H1(id='house-title', style={'textAlign': 'center'}),
        dcc.Graph(id='irradiance-graph', style={'height': '100%'})
    ]),
    html.Div(style={'height': '50vh', 'padding': '10px'}, children=[
        dcc.Graph(id='net-load-pv-graph', style={'height': '100%'})
    ]),
    dcc.Interval(id='interval-component', interval=1000, n_intervals=0),
    dcc.Store(id='selected_house_id', data=selected_house_id),
    dcc.Store(id='timestamp', data=timestamp),
    dcc.Store(id='end_timestamp', data=end_timestamp)






])"""


app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)
app.css.append_css({"external_url": "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"})

app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    html.Div(id="page-content"),html.Div(className='wrapper', children=[
        html.Div(className='content-wrapper', style={'margin-right': '5%', 'margin-left':'5%','margin-bottom':'5%', 'margin-top':'3%'}, children=[
            html.H1('Solar Insights', style={'padding-bottom': '2%'}, className='content-header'),
            html.Div(className='content', children=[
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-6', children=[
                            html.H3('Select Date'),
                            dcc.DatePickerRange(
                                id='date-time-range',
                                start_date=timestamp,#start_default,
                                end_date=end_timestamp,#end_default,
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
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-12', children=[
                            html.H2('PV Statistics'),
                            html.H3('State', className='text-light'),
                            dcc.Loading(
                                id="loading-1",
                                children=[
                                    dash_table.DataTable(
                                        id='stateDatatable',
                                        columns=[
                                            {'name': 'State', 'id': 'State', 'type': 'text'},
                                            {'name': 'Peak Net Load', 'id': 'Peak Net Load', 'type': 'text'},
                                            {'name': 'Min Net Load', 'id': 'Min Net Load', 'type': 'text'},
                                            {'name': 'Peak PV Generation', 'id': 'Peak PV Generation', 'type': 'text'},
                                            {'name': 'Peak Gross Load', 'id': 'Peak Gross Load', 'type': 'text'},
                                            {'name': 'Average Net Load', 'id': 'Average Net Load', 'type': 'text'},
                                            {'name': 'Average PV Generation', 'id': 'Average PV Generation', 'type': 'text'},
                                            {'name': 'Average Gross Load', 'id': 'Average Gross Load', 'type': 'text'},
                                            {'name': 'Peak PV Penetration', 'id': 'Peak PV Penetration', 'type': 'text'},
                                            {'name': 'Mean PV Penetration', 'id': 'Mean PV Penetration', 'type': 'text'}
                                        ],
                                        page_current=0,
                                        page_size=1,
                                        page_count=1,
                                        style_table={'overflowX': 'auto', 'color': '#6c757d', 'padding-bottom': '1%'},
                                    )
                                ]
                            )
                        ])
                    ])
                ]),
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-12', children=[
                            html.H3('County', className='text-light'),
                            dash_table.DataTable(
                                id='countyDatatable',
                                columns=[
                                    {'name': 'County', 'id': 'County', 'type': 'text'},
                                    {'name': 'Peak Net Load', 'id': 'Peak Net Load', 'type': 'text'},
                                    {'name': 'Min Net Load', 'id': 'Min Net Load', 'type': 'text'},
                                    {'name': 'Peak PV Generation', 'id': 'Peak PV Generation', 'type': 'text'},
                                    {'name': 'Peak Gross Load', 'id': 'Peak Gross Load', 'type': 'text'},
                                    {'name': 'Average Net Load', 'id': 'Average Net Load', 'type': 'text'},
                                    {'name': 'Average PV Generation', 'id': 'Average PV Generation', 'type': 'text'},
                                    {'name': 'Average Gross Load', 'id': 'Average Gross Load', 'type': 'text'},
                                    {'name': 'Peak PV Penetration', 'id': 'Peak PV Penetration', 'type': 'text'},
                                    {'name': 'Mean PV Penetration', 'id': 'Mean PV Penetration', 'type': 'text'}
                                ],
                                page_current=0,
                                page_size=1,
                                page_count=1,
                                style_table={'overflowX': 'auto', 'color': '#6c757d', 'padding-bottom': '1%'},
                            )
                        ])
                    ])
                ]),
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-12', children=[
                            html.H3('Household', className='text-light'),
                            dash_table.DataTable(
                                id='householdDatatable',
                                columns=[
                                    {'name': 'House', 'id': 'House', 'type': 'text'},
                                    {'name': 'Peak Net Load', 'id': 'Peak Net Load', 'type': 'text'},
                                    {'name': 'Min Net Load', 'id': 'Min Net Load', 'type': 'text'},
                                    {'name': 'Peak PV Generation', 'id': 'Peak PV Generation', 'type': 'text'},
                                    {'name': 'Peak Gross Load', 'id': 'Peak Gross Load', 'type': 'text'},
                                    {'name': 'Average Net Load', 'id': 'Average Net Load', 'type': 'text'},
                                    {'name': 'Average PV Generation', 'id': 'Average PV Generation', 'type': 'text'},
                                    {'name': 'Average Gross Load', 'id': 'Average Gross Load', 'type': 'text'},
                                    {'name': 'Peak PV Penetration', 'id': 'Peak PV Penetration', 'type': 'text'},
                                    {'name': 'Mean PV Penetration', 'id': 'Mean PV Penetration', 'type': 'text'}
                                ],
                                data=[],
                                style_table={'overflowX': 'auto', 'color': '#6c757d', 'padding-bottom': '1%'},
                            )
                        ])
                    ])
                ]),
                html.Div(className='container-fluid', children=[
                    html.Div(className='col-md-12', children=[
                        html.H2('Load Insights'),
                        html.Div(className='row', children=[
                            html.Div(className='col-md-12', children=[
                                html.H4('State-Level', className='text-light'),
                                dcc.Graph(figure={}, id='state_graph')
                            ])
                        ]),
                        html.Div(className='row', children=[
                            html.Div(className='col-md-12', children=[
                                html.H4('County-Level', className='text-light'),
                                dcc.Graph(figure={}, id='county_graph')
                            ])
                        ]),
                        html.Div(className='row', children=[
                            html.Div(className='col-md-12', children=[
                                html.H4('Household-Level', className='text-light'),
                                dcc.Graph(figure={}, id='household_graph')
                            ])
                        ])
                    ])
                ], style={'padding-top':'2%'})




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
    return f"Live PV Disaggregation for House {selected_house_id}"

"""@app.callback(
    Output('irradiance-graph', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('start-button', 'n_clicks')],
    [State('selected_house_id', 'data')]
)
def update_irradiance_graph(n_intervals, start_clicks, house_id):
    global historical_time_irr, historical_irr, timestamp
    print(end_timestamp)
    if start_clicks % 2 == 0:  # Even clicks or initial load
        return go.Figure()

    if timestamp >= end_timestamp or paused:
        raise dash.exceptions.PreventUpdate

    try:
        print(timestamp)
        irradiance = pgapi.getIrradiance(house_id, timestamp)
        historical_irr.append(irradiance)
    except:
        historical_irr.append(None)
    historical_time_irr.append(timestamp)

    visible_irr = historical_irr
    visible_time = historical_time_irr

    trace_irr = go.Scatter(
        x=visible_time,
        y=visible_irr,
        mode='lines+markers',
        name='Irradiance',
        line=dict(color='green')
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

    return {'data': [trace_irr], 'layout': layout}

@app.callback(
    Output('net-load-pv-graph', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('start-button', 'n_clicks'),
     Input('pause-button', 'n_clicks')],
    [State('selected_house_id', 'data')]
)
def update_net_load_pv_graph(n_intervals, start_clicks, pause_clicks, house_id):
    global historical_time_load, historical_net_load, historical_pv, timestamp, paused

    if paused:
        raise dash.exceptions.PreventUpdate
    elif start_clicks % 2 == 0:  # Even clicks or initial load or paused
        return go.Figure()

    if timestamp >= end_timestamp:
        raise dash.exceptions.PreventUpdate

    if not historical_time_load:
        historical_time_load.append(timestamp)
        historical_net_load.append(0)
        historical_pv.append(0)
    else:
        current_time = historical_time_load[-1] + pd.Timedelta(minutes=15)
        historical_time_load.append(current_time)

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

    net_load_trace = go.Scatter(
        x=historical_time_load,
        y=historical_net_load,
        mode='lines+markers',
        name='Net Load'
    )

    pv_trace = go.Scatter(
        x=historical_time_load,
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

    return {'data': [net_load_trace, pv_trace], 'layout': layout}
"""




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
    print('here2', start_clicks)
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


