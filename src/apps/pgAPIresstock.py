import random
import re
import pandas as pd
import numpy as np
import pdb
from pgdriver import pgConnector

pgh = pgConnector()

def getLastRecordingByHouse(house_ids, start_date, end_date):
    try:
        # TODO: update for multiple meters
        #query_house = f"select last_pv_load_prediction from smartmeter where house_id={house_id}"
        query_house = f"""
    SELECT
        n.load_value,
        p.pv_value,
        p.timestamp AS pv_timestamp,
        s.house_id
    FROM
        smartmeter s
    INNER JOIN
        net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN '{start_date}' AND '{end_date}'
    INNER JOIN
        pv_load_ts p ON s.meter_id = p.meter_id AND p.timestamp BETWEEN '{start_date}' AND '{end_date}'
    INNER JOIN
        house h on s.house_id = h.house_id
    WHERE
        s.house_id IN ({','.join(map(str, house_ids))})
        and h.data_type = 'resstock';
"""
        #print(query_house)
        data_pv = pgh._read(query_house)
        #print(data_pv)
        # TODO: replace with actual pv prediction once recording implemented
        return data_pv#random.uniform(-1, 0), random.uniform(0, 3)
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

def getLastRecordingByCounty(county, start_date, end_date):
    try:
        #print('county', county, start_date, end_date)
        # TODO: update for multiple meters
        #query_county = f"select sm.last_pv_load_prediction from house as h join smartmeter as sm on h.house_id = sm.house_id where h.county_id={county_id}"
        query_county = f"""SELECT
    n.load_value,
    p.pv_value,
    p.timestamp AS pv_timestamp,
    s.house_id
FROM
    smartmeter s
INNER JOIN
    net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN '{start_date}' AND '{end_date}'
INNER JOIN
    pv_load_ts p ON s.meter_id = p.meter_id AND p.timestamp BETWEEN '{start_date}' AND '{end_date}'
INNER JOIN
    house h ON s.house_id = h.house_id
INNER JOIN
    county c ON h.county_id = c.county_id
WHERE
    c.county = '{county}' and h.data_type='resstock';
"""
        data_pv = pgh._read(query_county)
        #print(data_pv)
        # TODO: replace with actual pv prediction once recording implemented
        return data_pv#random.uniform(-1, 0), random.uniform(0, 3)
    except Exception as e:
        print('Failed to get last recording by county ->', e)
        raise ConnectionError('Reading last recording by county failed -> '  + e)

def getLastRecordingByState(state, start_date, end_date):
    try:
        #print('state', state, start_date, end_date)
        # TODO: update for multiple meters
        #query_state = f"select sm.last_pv_load_prediction from house as h join smartmeter as sm on h.house_id = sm.house_id where h.state_id={state_id}"
        query_state = f"""SELECT
    n.load_value,
    p.pv_value,
    p.timestamp AS pv_timestamp,
    s.house_id
FROM
    smartmeter s
INNER JOIN
    net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN '{start_date}' AND '{end_date}'
INNER JOIN
    pv_load_ts p ON s.meter_id = p.meter_id AND p.timestamp BETWEEN '{start_date}' AND '{end_date}'
INNER JOIN
    house h ON s.house_id = h.house_id
INNER JOIN
    state st ON h.state_id = st.state_id
WHERE
    st.state = '{state}' and h.data_type='resstock';
"""
        data_pv = pgh._read(query_state)

        # TODO: replace with actual pv prediction once recording implemented
        return data_pv#random.uniform(-1, 0), random.uniform(0, 3)
    except Exception as e:
        print('Failed to get last recording by state ->', e)
        raise ConnectionError('Reading last recording by state failed -> '  + e)

def getGrossLoad(predicted_pv, net_load):
    return np.abs(predicted_pv) + net_load

def getAbsNetLoad(net_load):
    return np.abs(net_load)

def getPVPenetration(predicted_pv, gross_load):
    return np.abs(predicted_pv) / np.abs(gross_load)

def getPeakPV(predicted_pvs):
    return predicted_pvs.min()

def getPeakNetLoad(net_loads):
    return np.abs(net_loads).max()

def getAvgPV(predicted_pv):
    return predicted_pv.mean()
    
def getAvgNetLoad(net_loads):
    return net_loads.mean()

def getAvgGrossLoad(gross_loads):
    return gross_loads.mean()

def getPeakPVPenetration(peak_pv, avg_gross_load):
    return peak_pv / avg_gross_load

def getAvgPVPenetration(pv_penetration):
    return pv_penetration.mean()

def getStates():
    query_states = "select distinct(s.state) from state s join house h on s.state_id = h.state_id where h.data_type = 'resstock';"
    states = pgh._read(query_states)
    pattern = r'\((.*?)\)'

    extracted_states = [state[0] for state in states]

    return sorted(extracted_states)

def getCounties(selected_state):
    #query_counties = "select distinct c.county from county c join house h on c.county_id = h.county_id where h.data_type = 'pecan';"
    #query_counties = f"select distinct(c.county) from county c join house h on c.county_id = h.county_id join state s on h.state_id = s.state_id where h.data_type = 'pecan';"
    query_counties = f"select distinct(c.county) from county c join house h on c.county_id = h.county_id join state s on h.state_id = s.state_id where s.state = '{selected_state}' and h.data_type = 'resstock';"
    counties = pgh._read(query_counties)
    pattern = r'\((.*?)\)'

    extracted_counties = [county[0] for county in counties]

    return sorted(extracted_counties)

def getHouseIds(selected_state):#, selected_county):
    #query_house_ids=f"select distinct(h.house_id) from house h join state s on s.state_id = h.state_id join county c on c.county_id = h.county_id where h.data_type = 'pecan';"
    query_house_ids=f"select distinct(h.house_id) from house h join state s on s.state_id = h.state_id where s.state = '{selected_state}' and h.data_type = 'resstock';"
    house_ids = pgh._read(query_house_ids)

    extracted_house_ids = [house_id[0] for house_id in house_ids]
    #print(extracted_house_ids)
    return sorted(extracted_house_ids)

def getDefaultDates():
    query_dates = "select min(p.timestamp) from house h inner join smartmeter s on h.house_id = s.house_id inner join pv_load_ts p on s.meter_id = p.meter_id where h.data_type='resstock' limit 1;"
    #start_default = pgh._read(query_dates)[0]
    #print(start_default)
    #query_dates = "select date_trunc('day', min(p.timestamp)) + interval '1 day' as next_day from house h inner join smartmeter s on h.house_id = s.house_id join pv_load_ts p on s.meter_id = p.meter_id where h.data_type='resstock' limit 1;"
    #end_default = pgh._read(query_dates)[0]
    #start_default = start_default[0].strftime('%Y-%m-%d')
    #end_default = end_default[0].strftime('%Y-%m-%d')
    #return start_default, end_default
    return '2018-01-01 00:00:00', '2018-01-02 00:00:00'
