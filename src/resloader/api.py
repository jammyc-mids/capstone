import os
import pandas as pd

from . import utils

def get_spatial_tract_lookups(cache_dir='./s3_tmp'):
    """
        Get spatial tract lookups from S3

        Args:
            s3 (boto3.client): S3 client
            prefix (str): Prefix to filter keys
            cache_dir (str): Directory to cache the data
        
        Returns:
            pd.DataFrame: DataFrame with spatial tract lookups
    """
    key = utils.BASE_PATH + 'geographic_information/spatial_tract_lookup_table.csv'
    return utils.get_s3_df(key=key, cache_dir=cache_dir)

def get_individual_bldg_sim_keys(cache_dir='./s3_tmp', num_workers=1):
    """
        Get simulation keys from S3

        Args:
            s3 (boto3.client): S3 client
            prefix (str): Prefix to filter keys
        
        Returns:
            pd.DataFrame: DataFrame with simulation keys
    """
    PATH_NAME = os.path.join(cache_dir, 'individual_building_simulations.parquet')

    # found file locally
    if os.path.exists(PATH_NAME):
        return pd.read_parquet(PATH_NAME)

    # get from S3
    df_spatial = get_spatial_tract_lookups(cache_dir=cache_dir)
    prefixes = [
        utils.BASE_PATH
        + 'timeseries_individual_buildings/by_county/upgrade=0/'
        + f'county={specific}/' for specific in df_spatial['nhgis_county_gisjoin'].unique()
    ]

    out = (
        utils.s3_ls_parallel(prefixes, max_workers=num_workers)
        .assign(
            county=lambda x: x['key'].str.split('/').str[-2].str.split('=').str[-1],
            file=lambda x: x['key'].str.split('/').str[-1]
        )
        .merge(
            df_spatial[['nhgis_county_gisjoin', 'resstock_county_id']],
            left_on='county',
            right_on='nhgis_county_gisjoin',
            how='left'
        )
        .drop(columns=['nhgis_county_gisjoin'])
        .assign(size=lambda x: x['size'] / 1e6)
    )

    out.to_parquet(PATH_NAME)
    
    return out

def get_load_profile(
        key,
        include=None,
        exclude=None,
        add_total=True,
        cache_dir='./s3_tmp',
    ):
    """
        Get a load profile DataFrame from S3

        Args:
            key (str): Key of the object in the S3 bucket
            s3 (boto3.client): S3 client
            bucket_name (str): Name of the S3 bucket
            include (list): List of columns to include, if None, include all
            exclude (list): List of columns to exclude, if None, exclude none
            add_total (bool): Whether to add a total column, default is True
        
        Returns:
            pd.DataFrame: Load profile DataFrame
    """
    def include_column(col):
        return (
            'electric' in col
            and 'intensity' not in col
            and 'total' not in col
            and 'net' not in col
            and 'pv' not in col
        )

    return(
        utils.get_s3_df(key=key, cache_dir=cache_dir)
        .set_index('timestamp')
        .assign(**{'total':lambda x: x.sum(axis=1)} if add_total else {})
        .pipe(lambda x: x[[col for col in x.columns if include_column(col)]])
        .pipe(lambda x: x.rename(columns={col:col.split('.')[-2] for col in x.columns}))
        .pipe(lambda x: x[[col for col in x.columns if col not in exclude]] if exclude else x)
        .pipe(lambda x: x[[col for col in x.columns if col in include]] if include else x)
    )

