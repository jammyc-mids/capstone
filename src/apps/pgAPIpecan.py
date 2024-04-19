import random
import re
import pandas as pd
import numpy as np
import pdb
from pgdriver import pgConnector

pgh = pgConnector()

# Quick grab meters
query_meters = f"""select s.meter_id, h.house_id from smartmeter s join house h
    on h.house_id = s.house_id where h.data_type='pecan' and h.has_solar='t';""";
meters = pgh._read(query_meters)
meters_dict = {house_id: meter_id for meter_id, house_id in meters}

# Quick grab counties
query_counties = f"""select s.meter_id, c.county from smartmeter s join house h
    on h.house_id = s.house_id join county c on c.county_id = h.house_id where
    h.data_type='pecan' and h.has_solar='t';"""
counties = pgh._read(query_counties)
counties_dict = {meter_id: county for meter_id, county in counties}
county_keys = [county for _, county in counties]

# Quick grab states
query_states = f"""select sm.meter_id, s.state from smartmeter sm join house h
    on h.house_id = sm.house_id join state s on s.state_id = h.house_id where
    h.data_type='pecan' and h.has_solar='t';"""
states = pgh._read(query_states)
states_dict = {meter_id: state for meter_id, state in states}
state_keys = [state for _, state in states]

# Get net electrical load given house id and timestamp
def getNetLoad(house_id, timestamp):
    try:
        if isinstance(house_id, list):
            house_id=house_id[0]

        meter_id = meters_dict[house_id]
        net_load_query = f"""SELECT load_value FROM net_load_ts WHERE \
                             meter_id={meter_id} AND timestamp='{timestamp}';"""
        net_load = pgh._read(net_load_query)

        if net_load is not None and len(net_load) > 0:
            return net_load[0][0]
        return []
    except Exception as e:
        print('Failed to get last net load ->', e)
        raise ConnectionError('Reading last net load failed -> ' + e)

# Get solar signal given house id and timestamp
def getPVLoad(house_id, timestamp):
    try:
        if isinstance(house_id, list):
            house_id=house_id[0]

        meter_id = meters_dict[house_id]
        pv_load_query = f"""SELECT pv_value FROM pv_load_ts WHERE meter_id={meter_id} \
                            AND timestamp='{timestamp}';"""
        pv_load = pgh._read(pv_load_query)

        if pv_load is not None and len(pv_load) > 0:
            return pv_load[0][0]
        return []
    except Exception as e:
        print('Failed to get last PV load ->', e)
        raise ConnectionError('Reading last PV load failed -> ' + e)

# Get irradiance data given house_id and timestamp
def getIrradiance(house_id, timestamp):
    try:
        if isinstance(house_id, list):
            house_id=house_id[0]

        meter_id = meters_dict[house_id]
        irr_query = f"""SELECT w.irradiance_value FROM smartmeter_weather_map sw \
                        INNER JOIN weather w on sw.weather_id = w.weather_id WHERE \
                        w.timestamp = '{timestamp}' and sw.meter_id={meter_id};"""
        irr = pgh._read(irr_query)

        if irr is not None and len(irr) > 0:
            return irr[0][0]
        return []
    except Exception as e:
        print('Failed to get irradiance ->', e)
        raise ConnectionError('Reading irradiance failed -> ' + e)

# Get net load and solar signal given house id and start & end dates
def getLastRecordingByHouse(house_id, start_date, end_date):
    try:
        if isinstance(house_id, list):
            house_id=house_id[0]

        meter_id = meters_dict[np.int64(house_id)]
        query_house = f"""
            SELECT
                n.load_value,
                p.pv_value,
                n.timestamp,
                {house_id}
            FROM
                net_load_ts n
            INNER JOIN
                pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between \
                '{start_date}' and '{end_date}'
            WHERE
                n.timestamp between '{start_date}' and '{end_date}' and
                n.meter_id={meter_id}"""

        data_pv = pgh._read(query_house)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

# Get net loads and solar signals given a list of house ids and start & end dates
def getLastRecordingByHouses(house_ids, start_date, end_date):
    try:
        query_house = f"""
            SELECT
                n.load_value,
                p.pv_result,
                p.timestamp AS pv_timestamp,
                p.house_id--,
                --w.irradiance_value
            FROM
                smartmeter s
            INNER JOIN
                net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN
                '{start_date}' AND '{end_date}'
            INNER JOIN
                prediction_results p ON s.house_id = p.house_id AND p.timestamp
                BETWEEN '{start_date}' AND '{end_date}'
            INNER JOIN
                house h on s.house_id = h.house_id
            --INNER JOIN smartmeter_weather_map sw on s.meter_id = sw.meter_id
            --INNER JOIN weather w on sw.weather_id = w.weather_id and w.timestamp
            --BETWEEN '{start_date}'::date AND '{end_date}'::date
            WHERE
                s.house_id IN ({','.join(map(str, house_ids))})
                and h.data_type = 'pecan';
        """

        data_pv = pgh._read(query_house)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

# Get net load and solar signal across a time range and county
def getLastRecordingByCounty(county, start_date, end_date):
    try:
        query_county = f"""
            SELECT
                n.load_value,
                p.pv_result,
                p.timestamp AS pv_timestamp,
                p.house_id
            FROM
                smartmeter s
            INNER JOIN
                net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN
                '{start_date}' AND '{end_date}'
            INNER JOIN
                prediction_results p ON s.house_id = p.house_id AND p.timestamp
                BETWEEN '{start_date}' AND '{end_date}'
            INNER JOIN
                house h ON s.house_id = h.house_id
            INNER JOIN
                county c ON h.county_id = c.county_id
            WHERE
                c.county = '{county}' and h.data_type='pecan';
        """

        data_pv = pgh._read(query_county)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by county ->', e)
        raise ConnectionError('Reading last recording by county failed -> '  + e)

# Get net load and solar signal across a time range and state
def getLastRecordingByState(state, start_date, end_date):
    try:
        query_state = f"""
            SELECT
                n.load_value,
                p.pv_value,
                p.timestamp AS pv_timestamp,
                h.house_id
            FROM
                smartmeter s
            INNER JOIN
                net_load_ts n ON s.meter_id = n.meter_id AND n.timestamp BETWEEN
                '{start_date}' AND '{end_date}'
            INNER JOIN
                --prediction_results p ON s.house_id = p.house_id AND p.timestamp
                pv_load_ts p ON p.meter_id = s.meter_id AND p.timestamp
                BETWEEN '{start_date}' AND '{end_date}'
            INNER JOIN
                house h ON s.house_id = h.house_id
            INNER JOIN
                state st ON h.state_id = st.state_id
            WHERE
                st.state = '{state}' and h.data_type='pecan';
        """

        data_pv = pgh._read(query_state)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by state ->', e)
        raise ConnectionError('Reading last recording by state failed -> '  + e)

# Get states (with solar and from Pecan dataset)
def getStates():
    query_states = """select distinct(s.state) from state s join house h on
        s.state_id = h.state_id where h.data_type = 'pecan' and h.has_solar='t';"""
    states = pgh._read(query_states)

    extracted_states = [state[0] for state in states]

    return sorted(extracted_states)

# Get counties (with solar and from Pecan dataset)
def getCounties(selected_state):
    query_counties = f"""select distinct(c.county) from county c join house h on
        c.county_id = h.county_id join state s on h.state_id = s.state_id where
        h.has_solar='t' and s.state = '{selected_state}' and h.data_type = 'pecan';"""
    counties = pgh._read(query_counties)

    extracted_counties = [county[0] for county in counties]

    return sorted(extracted_counties)

# Get house ids (with solar and from Pecan dataset)
def getHouseIds(selected_state, selected_county):
    query_house_ids=f"""select distinct(h.house_id) from house h join state s on
        s.state_id = h.state_id join county c on c.county_id = h.county_id where
        s.state = '{selected_state}' and h.has_solar='t' and c.county = '{selected_county}'
        and h.data_type = 'pecan';"""
    house_ids = pgh._read(query_house_ids)
    extracted_house_ids = [house_id[0] for house_id in house_ids]
    return sorted(extracted_house_ids)

# Get default dates in db
def getDefaultDates():
    # Either retrieve earliest date from table given house in region or just predefine
    #query_dates = f"""select min(timestamp) from prediction_results where
    #    house_id={house_id};"""
    #start_default = pgh._read(query_dates)[0]
    #start_default = start_default[0]
    start_default = '2014-07-09'
    end_default = '2014-07-10'
    return start_default, end_default

# Get default date range for NY
def getDefaultDatesNY():
    return pd.to_datetime('2019-10-10'), pd.to_datetime('2019-10-11')
