"""Download sample data and stage into target folder"""

import sys
import os
import datetime as dt

import pandas as pd
import numpy as np

from botocore import UNSIGNED
from botocore.config import Config

os.chdir('/Users/darwish/Documents/Berkeley_Offline/W210/capstone/scripts') #<<<<<<<<<<<<<<<<<<<<<<< change this to your path
sys.path.append('../src')

from resloader.base import ResLoader #pylint: disable=import-error

# --------------------------------------------------------------------------------------------------
# Config Directories
BASE_DIR = '/Users/darwish/Documents/Berkeley_Offline/W210/capstone/' #<<<<<<<<<<<<<<<<<<<<<<<<<<<<< change this to your path
S3_CACHE_DIR = os.path.join(BASE_DIR, 's3_cache')

OUT_DIR = os.path.join(
    BASE_DIR,
    'data',
    'resstock',
    'staged',
    dt.datetime.now().strftime('%Y%m%d%H%M%S')
)

# Config Parameters
N_profiles = 50 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< change this to the number of profiles you want
N_partitions = 5 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< change this to the number of partitions you want

# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
# Constants

PROFILE_MAPPING = dict([
    ('timestamp', 'timestamp'),
    ('bldg_id', 'k__bldg_id'),
    ('out.outdoor_air_dryblub_temp.c', 'x__temperature'),
    ('out.electricity.clothes_dryer.energy_consumption', 'y__clothes_dryer'),
    ('out.electricity.clothes_washer.energy_consumption', 'y__clothes_washer'),
    ('out.electricity.cooling_fans_pumps.energy_consumption', 'y__cooling_fans_pumps'),
    ('out.electricity.cooling.energy_consumption', 'y__cooling'),
    ('out.electricity.dishwasher.energy_consumption', 'y__dishwasher'),
    ('out.electricity.freezer.energy_consumption', 'y__freezer'),
    ('out.electricity.heating_fans_pumps.energy_consumption', 'y__heating_fans_pumps'),
    ('out.electricity.heating.energy_consumption', 'y__heating'),
    ('out.electricity.hot_water.energy_consumption', 'y__hot_water'),
    ('out.electricity.lighting_exterior.energy_consumption', 'y__lighting_exterior'),
    ('out.electricity.lighting_interior.energy_consumption', 'y__lighting_interior'),
    ('out.electricity.range_oven.energy_consumption', 'y__range_oven'),
    ('out.electricity.refrigerator.energy_consumption', 'y__refrigerator'),
    ('out.electricity.total.energy_consumption', 'x__aggregate'),

])

METADATA_MAPPING = dict([
    ('bldg_id', 'k__bldg_id_2'),
    ('in.sqft', 'h__sqft'),
    ('in.state', 'h__state'),
    ('in.geometry_building_type_recs', 'h__geometry_building_type_recs'),
    ('in.bedrooms', 'h__bedrooms'),
    ('in.occupants', 'h__occupants'),
    ('in.vacancy_status', 'h__vacancy_status'),
    ('in.tenure', 'h__tenure'),
])

LOAD_LIST = [
    'y__clothes_dryer',
    'y__clothes_washer',
    'y__cooling',
    'y__dishwasher',
    'y__freezer',
    'y__heating',
    'y__hot_water',
    'y__lighting_exterior',
    'y__lighting_interior',
    'y__range_oven',
    'y__refrigerator',
]

OUT_COLS = (
    ['k__partition', 'k__bldg_id', 'k__dayofyear', 't__month', 't__day_of_week', 't__day_of_month']
    + [
        'h__state',
        'h__geometry_building_type_recs',
        'h__bedrooms',
        'h__occupants',
        'h__sqft',
        'h__is_occupied',
        'h__is_owner'
    ]
    + [f'{col}__{str(t).zfill(2)}' for col in ('x__aggregate', 'x__temperature') for t in range(96)]
    + [f'{col}__{str(t).zfill(2)}' for col in LOAD_LIST for t in range(96)]
)

# # snippet to generate value maps
# df_meta = pd.read_parquet(METADATA_PATH)
# STATE_MAPPING = {k:i for i, k in enumerate(sorted(x['in.state'].unique()))}
# GEOMETRY_MAPPING = {
#   k:i for i, k in enumerate(sorted(x['in.geometry_building_type_recs'].unique()))
# }

STATE_VALUE_MAP = {
    'AL': 0,
    'AR': 1,
    'AZ': 2,
    'CA': 3,
    'CO': 4,
    'CT': 5,
    'DC': 6,
    'DE': 7,
    'FL': 8,
    'GA': 9,
    'IA': 10,
    'ID': 11,
    'IL': 12,
    'IN': 13,
    'KS': 14,
    'KY': 15,
    'LA': 16,
    'MA': 17,
    'MD': 18,
    'ME': 19,
    'MI': 20,
    'MN': 21,
    'MO': 22,
    'MS': 23,
    'MT': 24,
    'NC': 25,
    'ND': 26,
    'NE': 27,
    'NH': 28,
    'NJ': 29,
    'NM': 30,
    'NV': 31,
    'NY': 32,
    'OH': 33,
    'OK': 34,
    'OR': 35,
    'PA': 36,
    'RI': 37,
    'SC': 38,
    'SD': 39,
    'TN': 40,
    'TX': 41,
    'UT': 42,
    'VA': 43,
    'VT': 44,
    'WA': 45,
    'WI': 46,
    'WV': 47,
    'WY': 48
}

BLDG_VALUE_MAP = {
    'Mobile Home': 0,
    'Multi-Family with 2 - 4 Units': 1,
    'Multi-Family with 5+ Units': 2,
    'Single-Family Attached': 3,
    'Single-Family Detached': 4
}
# --------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------
# helpers
def fix_timestamps(_df):
    """
        assume 2018-01-01 00:00:00 is a duplicate of 2018-01-01 00:15:00
        and drop 2019-01-01 00:00:00
    """
    start_timestamp = dt.datetime(2018, 1, 1, 0, 15, 0)
    desired_start_timestamp = dt.datetime(2018, 1, 1, 0, 0, 0)
    adjusted_rows = (
        _df
        .pipe(lambda x: x[x['timestamp'] == start_timestamp])
        .assign(timestamp = desired_start_timestamp)
    )


    return pd.concat(
        [_df.pipe(lambda x: x[x.timestamp.dt.year != 2019]), adjusted_rows],
        ignore_index=True
    )

def metl_and_pivot(_df):
    """Melt and pivot the dataframe to 1 day per row and 96 columns for each load"""
    id_cols = sorted([
        'k__bldg_id',
        'k__seq',
        'k__dayofyear',
        't__day_of_week',
        't__day_of_month',
        't__month'
    ])
    melt_cols = sorted(list(set(_df.columns) - set(id_cols)))

    return (
        _df
        .melt(id_vars=id_cols, value_vars=melt_cols, var_name='load_id', value_name='value')
        .assign(k__pivot_id = lambda x: x['load_id'] + '__' + x['k__seq'])
        .pivot_table(
            index=list(set(id_cols)-set(['k__seq'])),
            columns='k__pivot_id',
            values='value',
            aggfunc='first'
        )
        .reset_index()
        .reset_index(drop=True)
        .rename_axis(None, axis=1)
        .sort_values(['k__bldg_id', 'k__dayofyear'])
        .reset_index(drop=True)
    )
# --------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # adjust N_profiles to be divisible by N_partitions
    if N_profiles % N_partitions != 0:
        print('Warning: N_profiles is not divisible by N_partitions')
        N_profiles = N_profiles + (N_partitions - (N_profiles % N_partitions))
        print(f'Adjusted N_profiles to {N_profiles}')

    # init s3 loader
    loader = ResLoader(
        cache_dir=S3_CACHE_DIR,
        s3_config=Config(signature_version=UNSIGNED),
        sim_year='2022',
        sim_release='resstock_amy2018_release_1.1',
        sim_upgrade='0',
        sim_level='individual',
        sim_geo_agg='by_state',
    )

    # all simulation keys
    sim_keys = loader.get_sim_keys()

    sample_keys = sim_keys.sample(N_profiles, random_state=42)['key'].values

    files = [loader.key_to_path(k) for k in sample_keys]

    bldg_ids = [int(k.split('/')[-1].split('-')[0]) for k in sample_keys]

    # force download data
    for key in sample_keys:
        sample = loader.get_load_profile(key)

    df_meta = (
        loader.get_bldg_metadata()
        .loc[bldg_ids, :]
        .reset_index()
        .rename(columns=METADATA_MAPPING)
        [list(METADATA_MAPPING.values())]
        .assign(
            h__state = lambda x: x['h__state'].map(STATE_VALUE_MAP),
            h__geometry_building_type_recs = (
                lambda x: x['h__geometry_building_type_recs'].map(BLDG_VALUE_MAP)
            ),
            h__occupants = lambda x: (
                np.where(x['h__occupants'] == '10+', 10, x['h__occupants']).astype(int)
            ),
            h__is_occupied = lambda x: np.where(x['h__vacancy_status'] == 'Occupied', 1, 0),
            h__is_owner = lambda x: np.where(x['h__tenure'] == 'Owner', 1, 0),
        )
        .drop(['h__vacancy_status', 'h__tenure'], axis=1)
        .reset_index(drop=True)
        .sample(frac=1, random_state=42)
        # assign each building to 5 partitions randomly
        .assign(k__partition = lambda x: x.index % N_partitions)
    )

    print('Transforming profiles ...')

    df_out = (
        pd.concat(
            [pd.read_parquet(f).reset_index() for f in files],
            ignore_index=True
        )
        .rename(columns=PROFILE_MAPPING)
        [list(PROFILE_MAPPING.values())]
        .pipe(fix_timestamps)
        .astype({k:'float' for k in LOAD_LIST})
        .assign(
            y__cooling = lambda x: np.round(x['y__cooling'] + x['y__cooling_fans_pumps'], 3),
            y__heating = lambda x: np.round(x['y__heating'] + x['y__heating_fans_pumps'], 3),
        )
        .assign(
            # x__aggregate = lambda x: x[LOAD_LIST].sum(axis=1),
            k__seq = lambda x: (
                ((x['timestamp'].dt.hour * 4) + (x['timestamp'].dt.minute / 15))
                .astype(int).astype(str).str.zfill(2)
            ),
            k__dayofyear = lambda x: x['timestamp'].dt.dayofyear,
            t__day_of_week = lambda x: x['timestamp'].dt.dayofweek,
            t__day_of_month = lambda x: x['timestamp'].dt.day,
            t__month = lambda x: x['timestamp'].dt.month,
        )
        .drop(columns=['y__cooling_fans_pumps', 'y__heating_fans_pumps', 'timestamp'])
        .pipe(metl_and_pivot)
        .merge(
            df_meta,
            left_on='k__bldg_id',
            right_on='k__bldg_id_2',
            how='left'
        )
        .drop(columns='k__bldg_id_2')
        [OUT_COLS]
        .pipe(lambda x: x.astype({k:'float' for k in x.columns}))
        .sample(frac=1, random_state=42)
    )

    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)

    print(f'Writing to {OUT_DIR} ...')
    for grp, grp_df in df_out.groupby('k__partition'):
        print(f'Writing partition {int(grp)} ... ')
        (
            grp_df
            .sample(frac=1, random_state=1993)
            .to_parquet(os.path.join(OUT_DIR, f'partition={int(grp)}.parquet'), index=False)
        )
