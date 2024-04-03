import pandas as pd
import plotly.graph_objs as go
import dash
from dash import dash_table
from dash import dash_table, callback
import dash_core_components as dcc
import dash_html_components as html
import dash_mantine_components as dmc
from dash.dependencies import Input, Output


df = pd.read_csv('/Users/jq279/capstone/UI/dummy_data.csv')
df['Time'] = pd.to_datetime(df['Time'], format="%m/%d/%y %H:%M")
df = df.sort_values(by='Time')

df['Month'] = df['Time'].dt.month
df['Day'] = df['Time'].dt.day
df['Hour'] = df['Time'].dt.hour

df['Gross Load'] = df['PV Generation'].abs() + df['Net Load']
df['PV Penetration'] = df['PV Generation'] / df['Gross Load'].abs()
 
# ########################################################################

# select data for filter df

state_list_unique = df['State'].drop_duplicates().reset_index(drop=True)
county_list_unique = df['County'].drop_duplicates().reset_index(drop=True)

state_select = [{"label": item, "value": item} for item in state_list_unique]
county_select = [{"label": item, "value": item} for item in county_list_unique]
household_select = [{"label": item, "value": item}
                    for item in df['bldg_id'].drop_duplicates().reset_index(drop=True)]

app = dash.Dash(__name__)

app.layout = html.Div([

    dmc.Title('Overview', color="blue", size="h1"),
    dmc.Space(mt=30),
    dmc.Title('Select Date', color="blue", size="h3"),
    dmc.Grid([
        dmc.Col([
            dcc.DatePickerRange(
                id='date-time-range',
                start_date=df["Time"].min(),
                end_date=df["Time"].max(),
                display_format='YYYY-MM-DD',
                className='dateTimePicker'
            ),
        ], span=4),
    ]),
    dmc.Space(mt=30),
    dmc.Title('Select Area', color="blue", size="h3"),
    dmc.Grid([
        dmc.Col([
            dcc.Dropdown(
                id='tb-state-dropdown',
                placeholder="Select State",
                multi=True,
                options=state_select,
                value=''
            ),
        ], span=4),
        dmc.Col([
            dcc.Dropdown(
                id='tb-county-dropdown',
                placeholder="Select County",
                multi=True,
                options=county_select,
                value=''
            ),
        ], span=4),
        dmc.Col([
            dcc.Dropdown(
                id='tb-household-dropdown',
                placeholder="Select Household",
                multi=True,
                options=household_select,
                value=''
            ),
        ], span=4),
    ]),
    dmc.Space(mt=30),
    dmc.Title('Summary Statistics', color="blue", size="h2"),
    dmc.Space(mt=30),
    dmc.Title('State', color="blue", size="h3"),
    dmc.Grid([
        dmc.Col([
            dash_table.DataTable(
                id="stateDatatable",
                columns=[ 
                    {'name': 'Peak Net Load', 
                        'id': 'Peak Net Load', 'type': 'text'},
                    {'name': 'Min Net Load',
                        'id': 'Min Net Load', 'type': 'text'},
                    {'name': 'Peak PV Generation',
                        'id': 'Peak PV Generation', 'type': 'text'},
                    {'name': 'Peak Gross Load',
                        'id': 'Peak Gross Load', 'type': 'text'},
                    {'name': 'Average Net Load',
                        'id': 'Average Net Load', 'type': 'text'},
                    {'name': 'Average PV Generation',
                        'id': 'Average PV Generation', 'type': 'text'},
                    {'name': 'Average Gross Load',
                        'id': 'Average Gross Load', 'type': 'text'},                        
                    {'name': 'Peak PV Peneration',
                        'id': 'Peak PV Peneration', 'type': 'text'},
                    {'name': 'Average PV Peneration',
                        'id': 'Mean PV Peneration', 'type': 'text'}
                ],
                data=df.to_dict('records'),
                page_current=0,
                page_size=1,
                page_count=1,
                style_table={'overflowX': 'auto'},
            )
        ], span=12),
    ]),
    dmc.Space(mt=15),
    dmc.Title('County', color="blue", size="h3"),
    dmc.Grid([
        dmc.Col([
            dash_table.DataTable(
                id="countyDatatable",
                columns=[ 
                    {'name': 'Peak Net Load', 
                        'id': 'Peak Net Load', 'type': 'text'},
                    {'name': 'Min Net Load',
                        'id': 'Min Net Load', 'type': 'text'},
                    {'name': 'Peak PV Generation',
                        'id': 'Peak PV Generation', 'type': 'text'},
                    {'name': 'Peak Gross Load',
                        'id': 'Peak Gross Load', 'type': 'text'},
                    {'name': 'Average Net Load',
                        'id': 'Average Net Load', 'type': 'text'},
                    {'name': 'Average PV Generation',
                        'id': 'Average PV Generation', 'type': 'text'},
                    {'name': 'Average Gross Load',
                        'id': 'Average Gross Load', 'type': 'text'},                        
                    {'name': 'Peak PV Peneration',
                        'id': 'Peak PV Peneration', 'type': 'text'},
                    {'name': 'Average PV Peneration',
                        'id': 'Mean PV Peneration', 'type': 'text'}
                ],
                data=df.to_dict('records'),
                page_current=0,
                page_size=1,
                page_count=1,
                style_table={'overflowX': 'auto'},
            )
        ], span=12),
    ]),
    dmc.Space(mt=15),
    dmc.Title('Household', color="blue", size="h3"),
    dmc.Grid([
        dmc.Col([
            dash_table.DataTable(
                id="householdDatatable",
                columns=[ 
                    {'name': 'Peak Net Load', 
                        'id': 'Peak Net Load', 'type': 'text'},
                    {'name': 'Min Net Load',
                        'id': 'Min Net Load', 'type': 'text'},
                    {'name': 'Peak PV Generation',
                        'id': 'Peak PV Generation', 'type': 'text'},
                    {'name': 'Peak Gross Load',
                        'id': 'Peak Gross Load', 'type': 'text'},
                    {'name': 'Average Net Load',
                        'id': 'Average Net Load', 'type': 'text'},
                    {'name': 'Average PV Generation',
                        'id': 'Average PV Generation', 'type': 'text'},
                    {'name': 'Average Gross Load',
                        'id': 'Average Gross Load', 'type': 'text'},                        
                    {'name': 'Peak PV Peneration',
                        'id': 'Peak PV Peneration', 'type': 'text'},
                    {'name': 'Average PV Peneration',
                        'id': 'Mean PV Peneration', 'type': 'text'}
                ],
                data=df.to_dict('records'),
                page_current=0,
                page_size=1,
                page_count=1,
                style_table={'overflowX': 'auto'},
            )
        ], span=12),
    ]),
    dmc.Space(mt=30),
    dmc.Title('Graph', color="blue", size="h3"),
    dmc.Space(mt=30),
    dmc.Grid([
        dmc.Col([
            dmc.Title('State', color="blue", size="h4"),
            dcc.Graph(figure={}, id='state_graph')
        ], span=12),
        dmc.Col([
            dmc.Title('County', color="blue", size="h4"),
            dcc.Graph(figure={}, id='county_graph')
        ], span=12),
        dmc.Col([
            dmc.Title('Household', color="blue", size="h4"),
            dcc.Graph(figure={}, id='household_graph')
        ], span=12),
    ]),
])

@callback(Output('tb-county-dropdown', 'options'), [Input('tb-state-dropdown', 'value')])

def update_counties(selected_state):
    if not selected_state:
        return []
    county_unique_ret = df[(df['State'].isin(
        selected_state))]['County'].drop_duplicates().reset_index(drop=True)
    return [{"label": item, "value": item} for item in county_unique_ret]

@callback(Output('tb-household-dropdown', 'options'), [Input('tb-state-dropdown', 'value')], [Input('tb-county-dropdown', 'value')])

def update_counties(selected_state, selected_county):
    if not selected_state or not selected_county:
        return []

    selected_df = df
    if selected_state:
        selected_df = selected_df[(selected_df['State'].isin(selected_state))]

    if selected_county:
        selected_df = selected_df[(
            selected_df['County'].isin(selected_county))]

    state_unique_ret = selected_df['bldg_id'].drop_duplicates().reset_index(drop=True)

    return [{"label": item, "value": item} for item in state_unique_ret]

@app.callback(

    Output('householdDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-household-dropdown", "value"),
)

def update_household_table(start_date, end_date, household_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]

    # if nothing selected, make the table empty
    if not household_values:

        data_list = [
            {
                "Peak Net Load": "",
                "Min Net Load": "",
                "Peak PV Generation": "",
                "Peak Gross Load": "",
                "Average Net Load": "",
                "Average PV Generation": "",
                "Average Gross Load": "",
                "Peak PV Peneration": "",
                "Mean PV Peneration": ""
            }
        ]

        filtered_df = pd.DataFrame(data_list)
        return filtered_df.to_dict('records')

    filtered_df_household = filtered_df[(filtered_df['bldg_id'].isin(household_values))]

    # calculate peak load and pv
    grouped_household = filtered_df_household.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
    grouped_household['PV Penetration'] = grouped_household['PV Generation']/grouped_household['Gross Load']   
    peak_household_PV = round(grouped_household['PV Generation'].min(),3)
    peak_household_NL = round(grouped_household['Net Load'].max(),3)
    min_household_NL = round(grouped_household['Net Load'].min(),3)
    peak_to_peak_household_NL = round(peak_household_NL-min_household_NL,3)
    peak_household_GL = round(grouped_household['Gross Load'].max(),3)

    # avg load and pv
    avg_household_NL = round(grouped_household['Net Load'].mean(),3)
    avg_household_PV = round(grouped_household['PV Generation'].mean(),3)
    avg_household_GL = round(grouped_household['Gross Load'].mean(),3)

    # penetration
    peak_household_PV_pene = round(peak_household_PV/avg_household_GL,3)
    avg_household_PV_pene = round(grouped_household['PV Penetration'].mean(),3)


    data_list = [
        {
            "Peak Net Load": peak_household_NL,
            "Min Net Load": peak_to_peak_household_NL,
            "Peak PV Generation": peak_household_PV,
            "Peak Gross Load": peak_household_GL,
            "Average Net Load": avg_household_NL,
            "Average PV Generation": avg_household_PV,
            "Average Gross Load": avg_household_GL,
            "Peak PV Peneration": peak_household_PV_pene,
            "Mean PV Peneration": avg_household_PV_pene
        }
    ]
    filtered_df = pd.DataFrame(data_list)
    return filtered_df.to_dict('records')

@app.callback(
    Output('countyDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-county-dropdown", "value"),
)

def update_county_table(start_date, end_date, county_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]
 
    if not county_values:
        data_list = [
            {
                "Peak Net Load": "",
                "Min Net Load": "",
                "Peak PV Generation": "",
                "Peak Gross Load": "",
                "Average Net Load": "",
                "Average PV Generation": "",
                "Average Gross Load": "",
                "Peak PV Peneration": "",
                "Mean PV Peneration": ""
            }
        ]
        filtered_df = pd.DataFrame(data_list)
        return filtered_df.to_dict('records')
    
    filtered_df_county = filtered_df[(filtered_df['County'].isin(county_values))]
    
   # calculate peak load and pv
    grouped_county = filtered_df_county.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
    grouped_county['PV Penetration'] = grouped_county['PV Generation']/grouped_county['Gross Load']  
    peak_county_PV = round(grouped_county['PV Generation'].min(),3)
    peak_county_NL = round(grouped_county['Net Load'].max(),3)
    min_county_NL = round(grouped_county['Net Load'].min(),3)
    peak_to_peak_NL = round(peak_county_NL-min_county_NL,3)
    peak_county_GL = round(grouped_county['Gross Load'].max(),3)

    # avg load and pv
    avg_county_NL = round(grouped_county['Net Load'].mean(),3)
    avg_county_PV = round(grouped_county['PV Generation'].mean(),3)
    avg_county_GL = round(grouped_county['Gross Load'].mean(),3)

    # penetration
    peak_county_PV_pene = peak_county_PV/avg_county_GL
    avg_county_PV_pene = grouped_county['PV Penetration'].mean()

    data_list = [
        {
            "Peak Net Load": peak_county_NL,
            "Min Net Load": peak_to_peak_NL,
            "Peak PV Generation": peak_county_PV,
            "Peak Gross Load": peak_county_GL,
            "Average Net Load": avg_county_NL,
            "Average PV Generation": avg_county_PV,
            "Average Gross Load": avg_county_GL,
            "Peak PV Peneration": peak_county_PV_pene,
            "Mean PV Peneration": avg_county_PV_pene
        }
    ]
    filtered_df = pd.DataFrame(data_list)
    return filtered_df.to_dict('records')

@app.callback(
    Output('stateDatatable', 'data'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-state-dropdown", "value"),
)

def update_state_table(start_date, end_date, state_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]

    if not state_values:

        data_list = [
            {
                "Peak Net Load": "",
                "Min Net Load": "",
                "Peak PV Generation": "",
                "Peak Gross Load": "",
                "Average Net Load": "",
                "Average PV Generation": "",
                "Average Gross Load": "",
                "Peak PV Peneration": "",
                "Mean PV Peneration": ""
            }
        ]

        filtered_df = pd.DataFrame(data_list)
        return filtered_df.to_dict('records')

    filtered_df_state = filtered_df[(filtered_df['State'].isin(state_values))]

    # calculate peak load and pv
    grouped_state = filtered_df_state.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
    grouped_state['PV Penetration'] = grouped_state['PV Generation']/grouped_state['Gross Load']   
    peak_state_PV = round(grouped_state['PV Generation'].min(),3)
    peak_state_NL = round(grouped_state['Net Load'].max(),3)
    min_state_NL = round(grouped_state['Net Load'].min(),3)
    peak_to_peak_state_NL = peak_state_NL-min_state_NL
    peak_state_GL = round(grouped_state['Gross Load'].max(),3)

    # avg load and pv
    avg_state_NL = round(grouped_state['Net Load'].mean(),3)
    avg_state_PV = round(grouped_state['PV Generation'].mean(),3)
    avg_state_GL = round(grouped_state['Gross Load'].mean(),3)

    # penetration
    peak_state_PV_pene = round(peak_state_PV/avg_state_GL,3)
    avg_state_PV_pene = round(grouped_state['PV Penetration'].mean(),3)

    data_list = [
        {
            "Peak Net Load": peak_state_NL,
            "Min Net Load": peak_to_peak_state_NL,
            "Peak PV Generation": peak_state_PV,
            "Peak Gross Load": peak_state_GL,
            "Average Net Load": avg_state_NL,
            "Average PV Generation": avg_state_PV,
            "Average Gross Load": avg_state_GL,
            "Peak PV Peneration": peak_state_PV_pene,
            "Mean PV Peneration": avg_state_PV_pene
        }
    ]

    filtered_df = pd.DataFrame(data_list)
    return filtered_df.to_dict('records')


@callback(

    Output('state_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
    Input("tb-state-dropdown", "value"),

)


def update_state_graph(start_date, end_date, state_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]
    
    # Group by Month and HourMinute to calculate average 
    #filtered_df['HourMinute'] = filtered_df['Time'].dt.strftime('%H:%M')
    
    monthly_avg = pd.DataFrame([])

    if state_values:

        filtered_df_state = filtered_df[(filtered_df['State'].isin(state_values))]
        grouped_state = filtered_df_state.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
        grouped_state['PV Penetration'] = grouped_state['PV Generation']/grouped_state['Gross Load']
        grouped_state['Month'] = grouped_state['Time'].dt.month
        grouped_state['HourMinute'] = grouped_state['Time'].dt.strftime('%H:%M')
        monthly_avg = grouped_state.groupby(['Month', 'HourMinute'])['PV Penetration'].mean().reset_index()
    
    month_unique = monthly_avg['Month'].unique() if len(monthly_avg) else []
    
    # Plot

    fig = go.Figure()

    for month in month_unique:
        month_df = monthly_avg[monthly_avg['Month'] == month]
        fig.add_trace(go.Scatter(x=month_df['HourMinute'], y=month_df['PV Penetration'], mode='lines', name=pd.to_datetime(month, format='%m').strftime('%B')))
    fig.update_layout = go.Layout(title='Daily average PV Penetration Plot by Month')

    return fig 



@callback(

    Output('county_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
     Input("tb-state-dropdown", "value"),
    Input("tb-county-dropdown", "value"),

)

def update_county_graph(start_date, end_date, state_values, county_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]
    # Group by Month and HourMinute to calculate average 'Net Load'
    
    monthly_avg = pd.DataFrame([])

    if county_values:
        filtered_df_county = filtered_df[(filtered_df['County'].isin(county_values))]
        grouped_county = filtered_df_county.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
        grouped_county['PV Penetration'] = grouped_county['PV Generation']/grouped_county['Gross Load']
        grouped_county['Month'] = grouped_county['Time'].dt.month
        grouped_county['HourMinute'] = grouped_county['Time'].dt.strftime('%H:%M')
        monthly_avg = grouped_county.groupby(['Month', 'HourMinute'])['PV Penetration'].mean().reset_index()
    
    month_unique = monthly_avg['Month'].unique() if len(monthly_avg) else []

    # Plot

    fig = go.Figure()
    for month in month_unique:
        month_df = monthly_avg[monthly_avg['Month'] == month]
        fig.add_trace(go.Scatter(x=month_df['HourMinute'], y=month_df['PV Penetration'], mode='lines', name=pd.to_datetime(month, format='%m').strftime('%B')))
    #fig.update_layout = go.Layout(title='Daily average PV Penetration Plot by Month')
    return fig 



@callback(

    Output('household_graph', 'figure'),
    [Input('date-time-range', 'start_date'),
     Input('date-time-range', 'end_date')],
     Input("tb-state-dropdown", "value"),
    Input("tb-county-dropdown", "value"),
    Input("tb-household-dropdown", "value")

)

def update_household_graph(start_date, end_date, state_values, county_values, household_values):

    filtered_df = df[(df.Time >= start_date) & (df.Time <= end_date)]

    # Group by Month and HourMinute to calculate average 'Net Load'

    monthly_avg = pd.DataFrame([])

    if household_values:
        filtered_df_household = filtered_df[(filtered_df['bldg_id'].isin(household_values))]
        grouped_household = filtered_df_household.groupby('Time') [['PV Generation','Net Load','Gross Load']].sum().reset_index()
        grouped_household['PV Penetration'] = grouped_household['PV Generation']/grouped_household['Gross Load']
        grouped_household['Month'] = grouped_household['Time'].dt.month
        grouped_household['HourMinute'] = grouped_household['Time'].dt.strftime('%H:%M')
        monthly_avg = grouped_household.groupby(['Month', 'HourMinute'])['PV Penetration'].mean().reset_index()

    month_unique = monthly_avg['Month'].unique() if len(monthly_avg) else []

    # Plot

    fig = go.Figure()

    for month in month_unique:
        month_df = monthly_avg[monthly_avg['Month'] == month]
        fig.add_trace(go.Scatter(x=month_df['HourMinute'], y=month_df['PV Penetration'], mode='lines', name=pd.to_datetime(month, format='%m').strftime('%B')))
    #fig.update_layout = go.Layout(title='Daily average PV Penetration Plot by Month')

    return fig 

if __name__ == '__main__':

    app.run_server(debug=True)

