import dash
from dash import dash_table, callback
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
import pgAPIpecan as pgapi
import numpy as np

state_list_unique = pgapi.getStates()
state_select = [{"label": item, "value": item} for item in state_list_unique]
start_default, end_default = pgapi.getDefaultDates()
external_stylesheets = [dbc.themes.DARKLY]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, \
    suppress_callback_exceptions=True)
app.css.append_css({"external_url": \
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"})

app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    html.Div(id="page-content"),html.Div(className='wrapper', children=[
        html.Div(className='content-wrapper', style={'margin-right': '5%', \
                'margin-left':'5%','margin-bottom':'5%', 'margin-top':'3%'}, children=[
            html.H1('Solar Insights', style={'padding-bottom': '2%', \
                    'padding-left':'10px'}, className='content-header'),
            html.Div(className='content', children=[
                html.Div(className='container-fluid', children=[
                    html.Div(className='row', children=[
                        html.Div(className='col-md-6', children=[
                            html.H3('Select Date'),
                            dcc.DatePickerRange(
                                id='date-time-range',
                                start_date=start_default,
                                end_date=end_default,
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
                            html.H2('Load Statistics'),
                            html.H3('State', className='text-light'),
                            dcc.Loading(
                                id="loading-1",
                                children=[
                                    dash_table.DataTable(
                                        id='stateDatatable',
                                        columns=[
                                            {'name': 'State', 'id': 'State', \
                                                'type': 'text'},
                                            {'name': 'Peak Net Load', 'id': \
                                                'Peak Net Load', 'type': 'text'},
                                            {'name': 'Min Net Load', 'id': 'Min Net Load', \
                                                'type': 'text'},
                                            {'name': 'Peak PV Generation', 'id': \
                                                'Peak PV Generation', 'type': 'text'},
                                            {'name': 'Peak Gross Load', 'id': \
                                                'Peak Gross Load', 'type': 'text'},
                                            {'name': 'Average Net Load', 'id': \
                                                'Average Net Load', 'type': 'text'},
                                            {'name': 'Average PV Generation', 'id': \
                                                'Average PV Generation', 'type': 'text'},
                                            {'name': 'Average Gross Load', 'id': \
                                                'Average Gross Load', 'type': 'text'},
                                            {'name': 'Peak PV Penetration', 'id': \
                                                'Peak PV Penetration', 'type': 'text'},
                                            {'name': 'Mean PV Penetration', 'id': \
                                                'Mean PV Penetration', 'type': 'text'}
                                        ],
                                        page_current=0,
                                        page_size=1,
                                        page_count=1,
                                        style_table={'overflowX': 'auto', 'color': \
                                            '#6c757d', 'padding-bottom': '1%'},
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
                                    {'name': 'County', 'id': 'County', 'type': \
                                        'text'},
                                    {'name': 'Peak Net Load', 'id': 'Peak Net Load', \
                                        'type': 'text'},
                                    {'name': 'Min Net Load', 'id': 'Min Net Load', \
                                        'type': 'text'},
                                    {'name': 'Peak PV Generation', 'id': 'Peak PV Generation',
                                        'type': 'text'},
                                    {'name': 'Peak Gross Load', 'id': 'Peak Gross Load', \
                                        'type': 'text'},
                                    {'name': 'Average Net Load', 'id': 'Average Net Load', \
                                        'type': 'text'},
                                    {'name': 'Average PV Generation', 'id': 'Average PV Generation', \
                                        'type': 'text'},
                                    {'name': 'Average Gross Load', 'id': 'Average Gross Load', \
                                        'type': 'text'},
                                    {'name': 'Peak PV Penetration', 'id': 'Peak PV Penetration', \
                                        'type': 'text'},
                                    {'name': 'Mean PV Penetration', 'id': 'Mean PV Penetration', \
                                        'type': 'text'}
                                ],
                                page_current=0,
                                page_size=1,
                                page_count=1,
                                style_table={'overflowX': 'auto', 'color': '#6c757d', \
                                    'padding-bottom': '1%'},
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
                                    {'name': 'Peak Net Load', 'id': 'Peak Net Load', \
                                        'type': 'text'},
                                    {'name': 'Min Net Load', 'id': 'Min Net Load', \
                                        'type': 'text'},
                                    {'name': 'Peak PV Generation', 'id': 'Peak PV Generation', \
                                        'type': 'text'},
                                    {'name': 'Peak Gross Load', 'id': 'Peak Gross Load', \
                                        'type': 'text'},
                                    {'name': 'Average Net Load', 'id': 'Average Net Load', \
                                        'type': 'text'},
                                    {'name': 'Average PV Generation', 'id': 'Average PV Generation', \
                                        'type': 'text'},
                                    {'name': 'Average Gross Load', 'id': 'Average Gross Load', \
                                        'type': 'text'},
                                    {'name': 'Peak PV Penetration', 'id': 'Peak PV Penetration', \
                                        'type': 'text'},
                                    {'name': 'Mean PV Penetration', 'id': 'Mean PV Penetration', \
                                        'type': 'text'}
                                ],
                                data=[],
                                style_table={'overflowX': 'auto', 'color': '#6c757d', \
                                    'padding-bottom': '1%'},
                            )
                        ])
                    ])
                ]),
                html.Div(className='container-fluid', children=[
                    html.Div(className='col-md-12', children=[
                        html.H2('Load Trends'),
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

@app.callback(
    Output('tb-household-dropdown', 'disabled'),
    [Input('tb-state-dropdown', 'value'),
    Input('tb-county-dropdown', 'value')]
)
def update_household_dropdown_disabled(selected_state, selected_county):
    return not (selected_state and selected_county)


@app.callback(
    Output('date-time-range', 'start_date'),
    Output('date-time-range', 'end_date'),
    [Input('tb-state-dropdown', 'value')]
)
def update_date_range_default(state_values):
    if state_values == 'NY':
        return pgapi.getDefaultDatesNY()
    elif state_values == 'TX':
        return '2018-01-03', '2018-01-04'
    else:
        return pgapi.getDefaultDates()


@app.callback(
    Output('tb-household-dropdown', 'options'),
    [Input('tb-state-dropdown', 'value'),
    Input('tb-county-dropdown', 'value')]
)
def update_household_dropdown_options(selected_state, selected_county):
    if not selected_state and selected_county:
        return []

    if selected_state is not None and selected_county is not None and \
        len(selected_state) > 0 and len(selected_county) > 0:
        house_ids = pgapi.getHouseIds(selected_state, selected_county)
        options = [{"label": str(house_id), "value": str(house_id)} \
            for house_id in house_ids]
        return options
    else:
        return []

@app.callback(
    Output('tb-county-dropdown', 'disabled'),
    [Input('tb-state-dropdown', 'value')]
)
def update_county_dropdown_disabled(selected_state):
    return not selected_state

@app.callback(
    Output('tb-county-dropdown', 'options'),
    [Input('tb-state-dropdown', 'value')]
)
def update_county_dropdown_options(selected_state):
    if selected_state is not None and len(selected_state) > 0:
        county_list_unique = pgapi.getCounties(selected_state)
        county_select = [{"label": item, "value": item} for item in \
            county_list_unique]
        return county_select
    else:
        return []

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
    Output('stateDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-state-dropdown", "value"),
)

def update_state_table(start_date, end_date, state_values):
    if state_values is not None and len(state_values) > 0:
        items = pgapi.getLastRecordingByState(state_values, start_date, end_date)
        filtered_df_state = pd.DataFrame(items, columns=['Net Load', 'PV Generation', \
            'Timestamp', 'bldg_id'])
        filtered_df_state['Gross Load'] = round(filtered_df_state['PV Generation'].abs() + \
            filtered_df_state['Net Load'], 10)

    if not state_values:

        data_list = [
            {
                "State": "",
                "Peak Net Load": "",
                "Min Net Load": "",
                "Peak PV Generation": "",
                "Peak Gross Load": "",
                "Average Net Load": "",
                "Average PV Generation": "",
                "Average Gross Load": "",
                "Peak PV Penetration": "",
                "Mean PV Penetration": ""
            }
        ]

        filtered_df_state = pd.DataFrame(data_list)
        return filtered_df_state.to_dict('records')



    # calculate peak load and pv
    grouped_state = filtered_df_state.groupby('Timestamp') [['PV Generation', \
        'Net Load', 'Gross Load', 'bldg_id']].sum().reset_index()
    grouped_state['PV Penetration'] = round(grouped_state['PV Generation'] / grouped_state['Gross Load'], 3)
    peak_state_PV = round(grouped_state['PV Penetration'].min(), 3)
    peak_state_NL = round(grouped_state['Net Load'].max(), 3)
    min_state_NL = round(grouped_state['Net Load'].min(), 3)
    peak_to_peak_state_NL = round(peak_state_NL - min_state_NL, 3)
    peak_state_GL = round(grouped_state['Gross Load'].max(), 3)

    # avg load and pv
    avg_state_NL = round(grouped_state['Net Load'].mean(), 3)
    avg_state_PV = round(grouped_state['PV Penetration'].mean(), 3)
    avg_state_GL = round(grouped_state['Gross Load'].mean(), 3)

    # penetration
    peak_state_PV_pene = round(peak_state_PV / avg_state_GL, 3)
    avg_state_PV_pene = round(grouped_state['PV Penetration'].mean(), 3)

    data_list = [
        {
            "State": state_values,
            "Peak Net Load": round(peak_state_NL, 3),
            "Min Net Load": round(peak_to_peak_state_NL, 3),
            "Peak PV Generation": round(peak_state_PV, 3),
            "Peak Gross Load": round(peak_state_GL, 3),
            "Average Net Load": round(avg_state_NL, 3),
            "Average PV Generation": round(avg_state_PV, 3),
            "Average Gross Load": round(avg_state_GL, 3),
            "Peak PV Penetration": round(peak_state_PV_pene, 3),
            "Mean PV Penetration": round(avg_state_PV_pene, 3)
        }
    ]

    filtered_df = pd.DataFrame(data_list)
    return filtered_df.to_dict('records')


@app.callback(
    Output('state_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-state-dropdown", "value"),

)
def update_state_graph(start_date, end_date, state_values):
    if state_values is not None and len(state_values) > 0:
        items = pgapi.getLastRecordingByState(state_values, start_date, end_date)
        filtered_df_state = pd.DataFrame(items, columns=['Net Load', 'PV Generation', 'Timestamp', 'bldg_id'])
        filtered_df_state['Gross Load'] = round(filtered_df_state['PV Generation'].abs() \
            + filtered_df_state['Net Load'], 10)

    monthly_avg = pd.DataFrame([])

    if state_values:
        grouped_state = filtered_df_state.groupby('Timestamp') [['PV Generation','Net Load', \
            'Gross Load', 'bldg_id']].sum().reset_index()
        grouped_state['PV Penetration'] = grouped_state['PV Generation'] / grouped_state['Gross Load']
        grouped_state['Timestamp'] = pd.to_datetime(grouped_state['Timestamp'])
        grouped_state['Month'] = grouped_state['Timestamp'].dt.month
        grouped_state['HourMinute'] = grouped_state['Timestamp'].dt.strftime('%H:%M')
        monthly_avg = grouped_state.groupby(['Month', 'HourMinute'])\
            ['PV Penetration'].mean().reset_index()
        monthly_avg['Net Load'] = grouped_state['Net Load']
        monthly_avg['PV Generation'] = grouped_state['PV Generation']
        monthly_avg['Gross Load'] = grouped_state['Gross Load']
        monthly_avg['bldg_id'] = grouped_state['bldg_id']

    month_unique = monthly_avg['Month'].unique() if len(monthly_avg) else []

    fig = go.Figure()

    for month in month_unique:
        month_df = monthly_avg[monthly_avg['Month'] == month]
        fig.add_trace(go.Scatter(x=month_df['HourMinute'], y=month_df['Net Load'], \
            mode='lines', name=f"""Net Load for NY: {pd.to_datetime(month, format='%m').strftime('%B')}"""))
        fig.add_trace(go.Scatter(x=month_df['HourMinute'], y=month_df['Gross Load'], \
            mode='lines', name=f"""Gross Load for NY: {pd.to_datetime(month, format='%m').strftime('%B')}"""))


    return fig

@app.callback(
    Output('countyDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-county-dropdown", "value"),
)
def update_county_table(start_date, end_date, county_values):
    if county_values is not None and len(county_values) > 0:
        items = pgapi.getLastRecordingByCounty(county_values, start_date, end_date)
        filtered_df = pd.DataFrame(items, columns=['Net Load', 'PV Generation', \
            'Timestamp', 'bldg_id'])
        filtered_df['Gross Load'] = round(filtered_df['PV Generation'].abs() + \
            filtered_df['Net Load'], 10)

    if not county_values:
        data_list = [
            {
                "County": "",
                "Peak Net Load": "",
                "Min Net Load": "",
                "Peak PV Generation": "",
                "Peak Gross Load": "",
                "Average Net Load": "",
                "Average PV Generation": "",
                "Average Gross Load": "",
                "Peak PV Penetration": "",
                "Mean PV Penetration": ""
            }
        ]
        filtered_df = pd.DataFrame(data_list)
        return filtered_df.to_dict('records')

    filtered_df_county = filtered_df

   # calculate peak load and pv
    grouped_county = filtered_df_county.groupby('Timestamp')[['PV Generation','Net Load', \
        'Gross Load', 'bldg_id']].sum().reset_index()
    grouped_county['PV Penetration'] = round(grouped_county['PV Generation'] / \
        grouped_county['Gross Load'], 3)
    peak_county_PV = round(grouped_county['PV Generation'].min(), 3)
    peak_county_NL = round(grouped_county['Net Load'].max(), 3)
    min_county_NL = round(grouped_county['Net Load'].min(), 3)
    peak_to_peak_NL = round(peak_county_NL - min_county_NL, 3)
    peak_county_GL = round(grouped_county['Gross Load'].max(), 3)

    # avg load and pv
    avg_county_NL = round(grouped_county['Net Load'].mean(), 3)
    avg_county_PV = round(grouped_county['PV Generation'].mean(), 3)
    avg_county_GL = round(grouped_county['Gross Load'].mean(), 3)

    # penetration
    peak_county_PV_pene = round(peak_county_PV / avg_county_GL, 3)
    avg_county_PV_pene = round(grouped_county['PV Penetration'].mean(), 3)

    data_list = [
        {
            "County": county_values,
            "Peak Net Load": round(peak_county_NL, 3),
            "Min Net Load": round(peak_to_peak_NL, 3),
            "Peak PV Generation": round(peak_county_PV, 3),
            "Peak Gross Load": round(peak_county_GL, 3),
            "Average Net Load": round(avg_county_NL, 3),
            "Average PV Generation": round(avg_county_PV, 3),
            "Average Gross Load": round(avg_county_GL, 3),
            "Peak PV Penetration": round(peak_county_PV_pene, 3),
            "Mean PV Penetration": round(avg_county_PV_pene, 3)
        }
    ]
    filtered_df = pd.DataFrame(data_list)
    return filtered_df.to_dict('records')

@app.callback(
    Output('county_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
     Input("tb-state-dropdown", "value"),
    Input("tb-county-dropdown", "value"),

)
def update_county_graph(start_date, end_date, state_values, county_values):
    if county_values is not None and len(county_values) > 0:
        items = pgapi.getLastRecordingByCounty(county_values, start_date, end_date)
        filtered_df_county = pd.DataFrame(items, columns=['Net Load', 'PV Generation', 'Timestamp', 'bldg_id'])
        filtered_df_county['Gross Load'] = round(filtered_df_county['PV Generation'].abs() \
            + filtered_df_county['Net Load'], 10)

    monthly_avg = pd.DataFrame([])

    if county_values:
        grouped_county = filtered_df_county.groupby('Timestamp')[['PV Generation', \
            'Net Load', 'Gross Load', 'bldg_id']].sum().reset_index()
        grouped_county['PV Penetration'] = grouped_county['PV Generation'] / grouped_county['Gross Load']
        grouped_county['Timestamp'] = pd.to_datetime(grouped_county['Timestamp'])
        grouped_county['Month'] = grouped_county['Timestamp'].dt.month
        grouped_county['HourMinute'] = grouped_county['Timestamp'].dt.strftime('%H:%M')
        monthly_avg = grouped_county.groupby(['Month', 'HourMinute'])['PV Penetration'].mean().reset_index()
        monthly_avg['Net Load'] = grouped_county['Net Load']
        monthly_avg['PV Generation'] = grouped_county['PV Generation']
        monthly_avg['Gross Load'] = grouped_county['Gross Load']
        monthly_avg['bldg_id'] = grouped_county['bldg_id']

    month_unique = monthly_avg['Month'].unique() if len(monthly_avg) else []

    # Plot
    fig = go.Figure()
    for month in month_unique:
        month_df = monthly_avg[monthly_avg['Month'] == month]
        fig.add_trace(go.Scatter(x=monthly_avg['HourMinute'], y=monthly_avg['Net Load'], \
                mode='lines', name=f"""Net Load for Tompkins County: \
                    {pd.to_datetime(month, format='%m').strftime('%B')}""", hovertemplate='%{y:.2f}'))
        fig.add_trace(go.Scatter(x=monthly_avg['HourMinute'], y=monthly_avg['Gross Load'], \
            mode='lines', name=f"""Gross Load for Tompkins County: \
                {pd.to_datetime(month, format='%m').strftime('%B')}""", hovertemplate='%{y:.2f}'))
    return fig

@app.callback(

    Output('householdDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-household-dropdown", "value"),
)

def update_household_table(start_date, end_date, household_values):
    if household_values is not None and len(household_values) > 0:
        items = pgapi.getLastRecordingByHouses(household_values, start_date, end_date)
        filtered_df = pd.DataFrame(items, columns=['Net Load', 'PV Generation', \
            'Timestamp', 'bldg_id'])
    else:
        filtered_df = pd.DataFrame(columns=['Net Load', 'PV Generation', \
            'Timestamp', 'bldg_id'])

    result_list = []
    for house_id in household_values:
        house_df = filtered_df[filtered_df['bldg_id'] == np.int64(house_id)]
        # calculate statistics for each house
        if not house_df.empty:
            peak_net_load = house_df['Net Load'].max()
            min_net_load = house_df['Net Load'].min()
            peak_to_peak_net_load = peak_net_load - min_net_load
            peak_pv_generation = house_df['PV Generation'].max()
            peak_gross_load = peak_net_load + abs(peak_pv_generation)
            average_net_load = house_df['Net Load'].mean()
            average_pv_generation = house_df['PV Generation'].mean()
            average_gross_load = average_net_load + abs(average_pv_generation)
            peak_pv_penetration = peak_pv_generation / peak_gross_load
            mean_pv_penetration = average_pv_generation / average_gross_load

            result_list.append({
                "House": house_id,
                "Peak Net Load": round(peak_net_load, 3),
                "Min Net Load": round(peak_to_peak_net_load, 3),
                "Peak PV Generation": round(peak_pv_generation, 3),
                "Peak Gross Load": round(peak_gross_load, 3),
                "Average Net Load": round(average_net_load, 3),
                "Average PV Generation": round(average_pv_generation, 3),
                "Average Gross Load": round(average_gross_load, 3),
                "Peak PV Penetration": round(peak_pv_penetration, 3),
                "Mean PV Penetration": round(mean_pv_penetration, 3)
            })

    return result_list

@app.callback(
    Output('household_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
     Input("tb-state-dropdown", "value"),
    Input("tb-county-dropdown", "value"),
    Input("tb-household-dropdown", "value")

)
def update_household_graph(start_date, end_date, state_values, county_values, household_values):
    fig = go.Figure()
    for household_id in household_values:
        items = pgapi.getLastRecordingByHouse(household_id, start_date, end_date)
        print(len(items))
        if not items:
            continue
        filtered_df = pd.DataFrame(items, columns=['Net Load', 'PV Generation', \
            'Timestamp', 'bldg_id'])
        filtered_df['Gross Load'] = round(filtered_df['PV Generation'].abs() + \
            filtered_df['Net Load'], 10)

        print(filtered_df.head())
        monthly_avg = filtered_df.groupby('Timestamp') [['PV Generation', 'Net Load', \
            'Gross Load', 'bldg_id']].sum().reset_index()
        monthly_avg['PV Penetration'] = monthly_avg['PV Generation'] / monthly_avg['Gross Load']
        monthly_avg['Timestamp'] = pd.to_datetime(monthly_avg['Timestamp'])
        monthly_avg['Month'] = monthly_avg['Timestamp'].dt.month
        monthly_avg['HourMinute'] = monthly_avg['Timestamp'].dt.strftime('%H:%M')
        monthly_avg = monthly_avg.groupby(['Month', 'HourMinute'])['PV Penetration'].mean().reset_index()
        monthly_avg['bldg_id'] = filtered_df['bldg_id']
        bldg_id = monthly_avg['bldg_id'].iloc[0]
        monthly_avg['Net Load'] = filtered_df['Net Load']
        monthly_avg['PV Generation'] = filtered_df['PV Generation']
        monthly_avg['Gross Load'] = filtered_df['Gross Load']

        for month in monthly_avg['Month'].unique():
            month_df = monthly_avg[monthly_avg['Month'] == month]
            fig.add_trace(go.Scatter(x=monthly_avg['HourMinute'], y=monthly_avg['Net Load'], \
                mode='lines', name=f"""Net Load for house {bldg_id}: \
                    {pd.to_datetime(month, format='%m').strftime('%B')}""", hovertemplate='%{y:.2f}'))
            fig.add_trace(go.Scatter(x=monthly_avg['HourMinute'], y=monthly_avg['Gross Load'], \
                mode='lines', name=f"""Gross Load for house {bldg_id}: \
                    {pd.to_datetime(month, format='%m').strftime('%B')}""", hovertemplate='%{y:.2f}'))

    return fig

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", debug=True)
