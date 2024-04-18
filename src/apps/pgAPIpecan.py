import random
import re
import pandas as pd
import numpy as np
import pdb
from pgdriver import pgConnector

pgh = pgConnector()

query_meters = f"""select s.meter_id, h.house_id from smartmeter s join house h
    on h.house_id = s.house_id where h.data_type='pecan' and h.has_solar='t';""";
meters = pgh._read(query_meters)
#print(meters)
meters_dict = {house_id: meter_id for meter_id, house_id in meters}
query_counties = f"""select s.meter_id, c.county from smartmeter s join house h
    on h.house_id = s.house_id join county c on c.county_id = h.house_id where
    h.data_type='pecan' and h.has_solar='t';"""
counties = pgh._read(query_counties)
counties_dict = {meter_id: county for meter_id, county in counties}
county_keys = [county for _, county in counties]
query_states = f"""select sm.meter_id, s.state from smartmeter sm join house h
    on h.house_id = sm.house_id join state s on s.state_id = h.house_id where
    h.data_type='pecan' and h.has_solar='t';"""
states = pgh._read(query_states)
states_dict = {meter_id: state for meter_id, state in states}
state_keys = [state for _, state in states]


def getNetLoad(house_id, timestamp):
    try:
        #house_id=house_id[0]
        if isinstance(house_id, list):
            house_id=house_id[0]
        meter_id = meters_dict[house_id]
        net_load_query = f"""SELECT load_value FROM net_load_ts WHERE meter_id={meter_id} AND timestamp='{timestamp}';"""
        net_load = pgh._read(net_load_query)
        if net_load is not None and len(net_load) > 0:
            return net_load[0][0]
        return []
    except Exception as e:
        print('Failed to get last net load ->', e)
        raise ConnectionError('Reading last net load failed -> ' + e)

def getPVLoad(house_id, timestamp):
    try:
        #house_id=house_id[0]
        if isinstance(house_id, list):
            house_id=house_id[0]
        meter_id = meters_dict[house_id]
        pv_load_query = f"""SELECT pv_value FROM pv_load_ts WHERE meter_id={meter_id} AND timestamp='{timestamp}';"""
        pv_load = pgh._read(pv_load_query)
        if pv_load is not None and len(pv_load) > 0:
            return pv_load[0][0]
        return []
    except Exception as e:
        print('Failed to get last PV load ->', e)
        raise ConnectionError('Reading last PV load failed -> ' + e)    

def getIrradiance(house_id, timestamp):
    try:
        #house_id=house_id[0]
        if isinstance(house_id, list):
            house_id=house_id[0]
        meter_id = meters_dict[house_id]
        irr_query = f"""SELECT w.irradiance_value FROM smartmeter_weather_map sw INNER JOIN weather w on sw.weather_id = w.weather_id WHERE w.timestamp = '{timestamp}' and sw.meter_id={meter_id};"""
        irr = pgh._read(irr_query)
        #print(irr)
        if irr is not None and len(irr) > 0:
            return irr[0][0]
        return []
    except Exception as e:
        print('Failed to get irradiance ->', e)
        raise ConnectionError('Reading irradiance failed -> ' + e)


def getLastRecordingByHouseUser(house_id, timestamp): # used by stacked
    try:
        meter_id = meters_dict[house_id]
        #print('meter_id', meter_id)
        query_house = f"""
        SELECT
            n.load_value,
            p.pv_value,
            w.irradiance_value
        FROM
            net_load_ts n
        INNER JOIN
            pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp = '{timestamp}'
        INNER JOIN smartmeter_weather_map sw on n.meter_id = sw.meter_id
        INNER JOIN weather w on sw.weather_id = w.weather_id and w.timestamp = '{timestamp}'
        WHERE
            n.timestamp = '{timestamp}' and n.meter_id={meter_id};
        """
        data_pv = pgh._read(query_house)
        #print(data_pv)
        return data_pv#[0]
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

def getLastRecordingByHouseUserIrr(house_id, timestamp): # used by stacked
    try:
        meter_id = meters_dict[house_id]
        #print('meter_id', meter_id)
        query_house = f"""
        SELECT
            w.irradiance_value
        FROM smartmeter_weather_map sw
        INNER JOIN weather w on sw.weather_id = w.weather_id --and w.timestamp = '{timestamp}'
        WHERE
            w.timestamp = '{timestamp}'::date and sw.meter_id={meter_id};
        """
        #query_house = "select load_value from net_load_ts limit 1;"
        data_pv = pgh._read(query_house)
        #print(data_pv)
        return data_pv[0]
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

def getLastRecordingByHouseUserLoad(house_id, timestamp): # used by stacked
    try:
        #print(timestamp)
        meter_id = meters_dict[house_id]
        #print('meter_id', meter_id)
        query_house = f"""
        SELECT
            n.load_value,
            p.pv_value
        FROM
            net_load_ts n
        INNER JOIN
            pv_load_ts p ON n.meter_id = p.meter_id --AND p.timestamp = '{timestamp}'
        WHERE
            n.timestamp = '{timestamp}'::date and n.meter_id={meter_id};
        """
        #print(query_house)
        data_pv = pgh._read(query_house)
        #print(len(data_pv))
        #print('load', data_pv)
        return data_pv#[0]
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

def getLastRecordingByHouseUserMain(house_id, timestamp1, timestamp2):
    try:
        meter_id = meters_dict[house_id]#np.int64(house_id)]
        #print('meter_id', meter_id)
        query_house = f"""
            SELECT
                n.load_value,
                p.pv_value,
                n.timestamp,
                #{house_id},
                w.irradiance_value
            FROM
                net_load_ts n
            INNER JOIN
                pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between '{timestamp1}'
                and '{timestamp2}'
            INNER JOIN smartmeter_weather_map sw on n.meter_id = sw.meter_id
            INNER JOIN weather w on sw.weather_id = w.weather_id and w.timestamp between
            '{timestamp1}' and '{timestamp2}'
            WHERE
                n.timestamp between '{timestamp1}' and '{timestamp2}' and
                n.meter_id={meter_id};
        """

        query_house = f"""
            SELECT
                n.load_value,
                p.pv_value,
                n.timestamp,
                {house_id},
                w.irradiance_value
            FROM
                net_load_ts n
            INNER JOIN
                pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between '{timestamp1}'
                and '{timestamp2}'
            INNER JOIN smartmeter_weather_map sw on n.meter_id = sw.meter_id
            INNER JOIN weather w on sw.weather_id = w.weather_id and w.timestamp between
            '{timestamp1}' and '{timestamp2}'
            WHERE
                n.timestamp between '{timestamp1}' and '{timestamp2}' and
                n.meter_id={meter_id};
        """

        query_house = f"""
            SELECT
                n.load_value,
                p.pv_value,
                n.timestamp
            FROM
                net_load_ts n
            INNER JOIN
                pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between '{timestamp1}' and '{timestamp2}'
            WHERE
                n.timestamp between '{timestamp1}' and '{timestamp2}' and
                n.meter_id={meter_id}"""



        data_pv = pgh._read(query_house)

        return data_pv#[0]
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)


def getLastRecordingByHouse(house_id, start_date, end_date):
    try:
        #print('house_id', house_id)
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
                pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between '{start_date}' and '{end_date}'
            WHERE
                n.timestamp between '{start_date}' and '{end_date}' and
                n.meter_id={meter_id}"""
        data_pv = pgh._read(query_house)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)


def getLastRecordingByHouses(house_ids, start_date, end_date):
    try:
        #print('house_ids', house_ids)
        query_house = f"""
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
                prediction_results p ON s.house_id = p.house_id AND p.timestamp BETWEEN
                '{start_date}' AND '{end_date}'
            INNER JOIN
                house h on s.house_id = h.house_id
            WHERE
                s.house_id IN ({','.join(map(str, house_ids))})
                and h.data_type = 'pecan';
        """
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
      
        results = []
        for house_id in house_ids:
            #print('hurrr', house_id, meters_dict[np.int64(house_id)])
            meter_id = meters_dict[np.int64(house_id)]
            query_h = f"""
                SELECT
                    n.load_value,
                    p.pv_value,
                    p.timestamp,
                    {house_id},
                    w.irradiance_value
                FROM
                    net_load_ts n
                INNER JOIN
                    pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp
                    BETWEEN '{start_date}' and '{end_date}'
                INNER JOIN smartmeter_weather_map sw on n.meter_id = sw.meter_id
                INNER JOIN weather w on sw.weather_id = w.weather_id and w.timestamp
                BETWEEN '{start_date}' and '{end_date}'
                WHERE
                    n.meter_id = {meters_dict[np.int64(house_id)]};
            """
            
            query_h = f"""
                SELECT
                    n.load_value,
                    p.pv_value,
                    n.timestamp,
                    {house_id}
                FROM
                    net_load_ts n
                INNER JOIN
                    pv_load_ts p ON n.meter_id = p.meter_id AND p.timestamp between '{start_date}' and '{end_date}'
                WHERE
                    n.timestamp between '{start_date}' and '{end_date}' and
                    n.meter_id={meter_id}"""
            #results = results.extend(pgh._read(query_h))
        data_pv = pgh._read(query_house)
        return data_pv
    except Exception as e:
        print('Failed to get last recording by house ->', e)
        raise ConnectionError('Reading last recording by house failed -> '  + e)

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

def getLastRecordingByState(state, start_date, end_date):
    try:
        query_state = f"""
            SELECT
                n.load_value,
                --p.pv_result,
                p.pv_value,
                p.timestamp AS pv_timestamp,
                --p.house_id
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



        #print(query_state)
        data_pv = pgh._read(query_state)

        return data_pv
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
    query_states = """select distinct(s.state) from state s join house h on
        s.state_id = h.state_id where h.data_type = 'pecan' and h.has_solar='t';"""
    states = pgh._read(query_states)

    extracted_states = [state[0] for state in states]

    return sorted(extracted_states)

def getCounties(selected_state):
    #print(type(selected_state))
    query_counties = f"""select distinct(c.county) from county c join house h on
        c.county_id = h.county_id join state s on h.state_id = s.state_id where
        h.has_solar='t' and s.state = '{selected_state}' and h.data_type = 'pecan';"""
    counties = pgh._read(query_counties)
    #print('getting counties, ', len(counties))

    extracted_counties = [county[0] for county in counties]
    #print(extracted_counties)

    return sorted(extracted_counties)

def getHouseIds(selected_state, selected_county):
    query_house_ids=f"""select distinct(h.house_id) from house h join state s on
        s.state_id = h.state_id join county c on c.county_id = h.county_id where
        s.state = '{selected_state}' and h.has_solar='t' and c.county = '{selected_county}'
        and h.data_type = 'pecan';"""
    house_ids = pgh._read(query_house_ids)
    extracted_house_ids = [house_id[0] for house_id in house_ids]
    return sorted(extracted_house_ids)

def getDefaultDates():
    query_dates = "select min(timestamp) from pv_load_ts where meter_id=5068;"
    #start_default = pgh._read(query_dates)[0]
    #print('START DATE', start_default)
    query_dates = """select date_trunc('day', min(timestamp)) + interval '1 day'
        as next_day from pv_load_ts where meter_id=5068;"""
    #end_default = pgh._read(query_dates)[0]
    #start_default = start_default[0].strftime('%Y-%m-%d')
    #end_default = end_default[0].strftime('%Y-%m-%d')
    #print(type(start_default), type(end_default))
    start_default = '2014-07-09'
    end_default = '2014-07-10'
    return start_default, end_default

def getDefaultDatesNY():
    query_dates = "select min(timestamp) from pv_load_ts where meter_id=5068;"
    #start_default = pgh._read(query_dates)[0]
    query_dates = """select date_trunc('day', min(timestamp)) + interval '1 day'
        as next_day from pv_load_ts where meter_id=5068;"""
    #end_default = pgh._read(query_dates)[0]
    #start_default = start_default[0].strftime('%Y-%m-%d')
    #end_default = end_default[0].strftime('%Y-%m-%d')
    #print(type(start_default), type(end_default))
    start_default = '2014-07-09'
    end_default = '2014-07-10'

    return pd.to_datetime('2019-10-10'), pd.to_datetime('2019-10-11')
    

def getDefaultDatess(house_id):
    query_dates = f"""select min(timestamp) from prediction_results where
        house_id={house_id};"""
    start_default = pgh._read(query_dates)[0]
    start_default = start_default[0]#.strftime('%Y-%m-%d')
    #print(start_default)
    #print(type(start_default))
    return start_default


