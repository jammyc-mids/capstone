import os
import functools

import concurrent.futures
import multiprocessing
import threading

import pandas as pd
import numpy as np

import boto3
import botocore

class ResLoader():
    """A class to load resources from ressotck S3 bucket"""
    SIM_TREE = {
        '2021':{
            'resstock_amy2018_release_1': {
                'aggregate':{
                    'by_county':True,
                    'by_state':True,
                },
                'individual':{
                    'by_county':True,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                },
                'weather_prefix':'2021/resstock_amy2018_release_1/weather/amy2018/',
            },
            'resstock_tmy3_release_1':{
                'aggregate':{
                    'by_county':True,
                    'by_state':True,
                },
                'individual':{
                    'by_county':True,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                },
                'weather_prefix':'2021/resstock_tmy3_release_1/weather/tmy3/',
            },
        },
        '2022':{
            'resstock_amy2018_release_1': {
                'aggregate':{
                    'by_county':False,
                    'by_state':True,
                },
                'individual':{
                    'by_county':False,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                    '1': 'Basic enclosure',
                    '2': 'Enhanced enclosure',
                    '3': 'Heat pumps, min-efficiency, electric backup',
                    '4': 'Heat pumps, high-efficiency, electric backup',
                    '5': 'Heat pumps, min-efficiency, existing heating as backup',
                    '6': 'Heat pump water heaters',
                    '7': 'Whole-home electrification, min efficiency',
                    '8': 'Whole-home electrification, high efficiency',
                    '9': 'Whole-home electrification, high efficiency + basic enclosure package (packages 1 & 8)',
                    '10': 'Whole-home electrification, high efficiency + enhanced enclosure package (packages 2 & 8)'
                },
                'weather_prefix':'2022/resstock_amy2018_release_1/weather/',
            },
            'resstock_amy2018_release_1.1': {
                'aggregate':{
                    'by_county':False,
                    'by_state':True,
                },
                'individual':{
                    'by_county':False,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                    '1': 'Basic enclosure',
                    '2': 'Enhanced enclosure',
                    '3': 'Heat pumps, min-efficiency, electric backup',
                    '4': 'Heat pumps, high-efficiency, electric backup',
                    '5': 'Heat pumps, min-efficiency, existing heating as backup',
                    '6': 'Heat pump water heaters',
                    '7': 'Whole-home electrification, min efficiency',
                    '8': 'Whole-home electrification, high efficiency',
                    '9': 'Whole-home electrification, high efficiency + basic enclosure package (packages 1 & 8)',
                    '10': 'Whole-home electrification, high efficiency + enhanced enclosure package (packages 2 & 8)'
                },
                'weather_prefix':'2022/resstock_amy2018_release_1/weather/'
            },
            'resstock_tmy3_release_1':{
                'aggregate':{
                    'by_county':False,
                    'by_state':False,
                },
                'individual':{
                    'by_county':False,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                    '1': 'Basic enclosure',
                    '2': 'Enhanced enclosure',
                    '3': 'Heat pumps, min-efficiency, electric backup',
                    '4': 'Heat pumps, high-efficiency, electric backup',
                    '5': 'Heat pumps, min-efficiency, existing heating as backup',
                    '6': 'Heat pump water heaters',
                    '7': 'Whole-home electrification, min efficiency',
                    '8': 'Whole-home electrification, high efficiency',
                    '9': 'Whole-home electrification, high efficiency + basic enclosure package (packages 1 & 8)',
                    '10': 'Whole-home electrification, high efficiency + enhanced enclosure package (packages 2 & 8)'
                },
                'weather_prefix':'2022/resstock_tmy3_release_1/weather/',
            },
            'resstock_tmy3_release_1.1':{
                'aggregate':{
                    'by_county':False,
                    'by_state':True,
                },
                'individual':{
                    'by_county':False,
                    'by_state':True,
                },
                'upgrades':{
                    '0': 'Baseline',
                    '1': 'Basic enclosure',
                    '2': 'Enhanced enclosure',
                    '3': 'Heat pumps, min-efficiency, electric backup',
                    '4': 'Heat pumps, high-efficiency, electric backup',
                    '5': 'Heat pumps, min-efficiency, existing heating as backup',
                    '6': 'Heat pump water heaters',
                    '7': 'Whole-home electrification, min efficiency',
                    '8': 'Whole-home electrification, high efficiency',
                    '9': 'Whole-home electrification, high efficiency + basic enclosure package (packages 1 & 8)',
                    '10': 'Whole-home electrification, high efficiency + enhanced enclosure package (packages 2 & 8)'
                },
                'weather_prefix':'2022/resstock_tmy3_release_1.1/weather/',
            },
        }
    }

    @classmethod
    def get_sim_years(cls):
        return list(cls.SIM_TREE.keys())

    @classmethod
    def get_sim_names(cls, sim_year):
        if sim_year not in cls.SIM_TREE:
            raise ValueError(f'sim_year must be one of {cls.get_sim_years()}')

        return list(cls.SIM_TREE[sim_year].keys())

    @classmethod
    def get_sim_aggregate(cls, sim_year, sim_release):
        if sim_year not in cls.SIM_TREE:
            raise ValueError(f'sim_year must be one of {cls.get_sim_years()}')

        if sim_release not in cls.SIM_TREE[sim_year]:
            raise ValueError(f'sim_release must be one of {cls.get_sim_names(sim_year)}')

        return cls.SIM_TREE[sim_year][sim_release]['aggregate']

    @classmethod
    def get_sim_individual(cls, sim_year, sim_release):
        if sim_year not in cls.SIM_TREE:
            raise ValueError(f'sim_year must be one of {cls.get_sim_years()}')

        if sim_release not in cls.SIM_TREE[sim_year]:
            raise ValueError(f'sim_release must be one of {cls.get_sim_names(sim_year)}')

        return cls.SIM_TREE[sim_year][sim_release]['individual']

    @classmethod
    def get_sim_upgrades(cls, sim_year, sim_release):
        if sim_year not in cls.SIM_TREE:
            raise ValueError(f'sim_year must be one of {cls.get_sim_years()}')

        if sim_release not in cls.SIM_TREE[sim_year]:
            raise ValueError(f'sim_release must be one of {cls.get_sim_names(sim_year)}')

        return cls.SIM_TREE[sim_year][sim_release]['upgrades']

    def __init__(
            self,
            cache_dir,
            s3_config,
            sim_year,
            sim_release,
            sim_level,
            sim_geo_agg,
            sim_upgrade
        ):
        """
        Initialize the ResLoader class

        Args:
            cache_dir (str): Directory to cache the object locally
            s3_config (botocore.config): Config for the S3 client
            sim_year (str): Year of the simulation
            sim_release (str): Release of the simulation
            sim_level (str): Level of the simulation, either 'aggregate' or 'individual'
            sim_geo_agg (str): Geographic aggregation level of the simulation, either 'by_county' or 'by_state'
            sim_upgrade (int): Upgrade level of the simulation
        """

        self.bucket_name = 'oedi-data-lake'

        self.s3_config = s3_config
        self.cache_dir = cache_dir

        # run checks on sim_year, sim_release, sim_upgrade
        if sim_year not in self.get_sim_years():
            raise ValueError(f'sim_year must be one of {self.get_sim_years()}')

        if sim_release not in self.get_sim_names(sim_year):
            raise ValueError(f'sim_release must be one of {self.get_sim_names(sim_year)}')

        if sim_upgrade not in self.get_sim_upgrades(sim_year, sim_release):
            raise ValueError(f'sim_upgrade must be one of {self.get_sim_upgrades(sim_year, sim_release)}')

        if sim_level not in ['aggregate', 'individual']:
            raise ValueError('sim_level must be either "aggregate" or "individual"')

        if sim_geo_agg not in ['by_county', 'by_state']:
            raise ValueError('sim_geo_agg must be either "by_county" or "by_state"')

        if not self.SIM_TREE[sim_year][sim_release][sim_level][sim_geo_agg]:
            raise ValueError(f'No data for {sim_year} {sim_release} {sim_level} {sim_geo_agg}')

        self.sim_year = sim_year
        self.sim_release = sim_release
        self.sim_upgrade = sim_upgrade
        self.sim_level = sim_level
        self.sim_geo_agg = sim_geo_agg

        self.root = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/'
        self.base_prefix = self.root + f'{sim_year}/{sim_release}/'
        print(self.base_prefix)

        _sim_level_folder = 'timeseries_individual_buildings' if sim_level == 'individual' else 'timeseries_aggregates'
        
        self.sim_prefix = self.base_prefix + f'{_sim_level_folder}/{sim_geo_agg}/upgrade={sim_upgrade}/'
        # handle cases where upgrade folder is not present
        if (self.sim_year == '2021'
            and self.sim_release == 'resstock_amy2018_release_1'
            and self.sim_level == 'aggregate'
        ):
            self.sim_prefix = self.base_prefix + f'{_sim_level_folder}/{sim_geo_agg}/'

        # create dirs
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, *self.base_prefix.split('/'), '_staged'), exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, *self.sim_prefix.split('/')), exist_ok=True)

        self._df_meta_extract = None

    # utils
    # ---------------------------------------------------------------------------------------------- 
    @functools.lru_cache()
    def _get_s3_client(self, _):
        """
        credit: https://gist.github.com/sjakthol/a635e45b2da344b61db7d05bb6a0fbd3
        
        Get thread-specific S3 client.
        Args:
            _ (int): thread identifier
        Returns:
            (S3.Client) botocore S3 client
        """
        session = botocore.session.get_session()
        return session.create_client('s3', config=self.s3_config)


    def _list_objects_v2(self, Bucket, Prefix='/', Delimiter='/'): #pylint: disable=invalid-name
        """
        credit: https://gist.github.com/sjakthol/a635e45b2da344b61db7d05bb6a0fbd3
        
        List ALL objects of bucket in given prefix.
        Args:
            :Bucket (str): the name of the bucket to list
            :Prefix (str, optional): a prefix of the bucket to list (Default: None)
            :Delimiter (str, optional): delimeter used to separate directories in S3 (Default: /)
        Returns:
            obj: The list of objects and directories under the given Prefix::
                {
                    'Contents': [{
                        'Key': 'prefix/file.json',
                        'LastModified': datetime.datetime(2018, 12, 13, 14, 15, 16, 000000),
                        'ETag': '"58bcd9641b1176ea012b6377eb5ce050"'
                        'Size': 262756,
                        'StorageClass': 'STANDARD'
                    }],
                    'CommonPrefixes': [{
                        'Prefix': 'prefix/another/
                    }]
                }
        """
        s3_client = self._get_s3_client(threading.current_thread())
        paginator = s3_client.get_paginator('list_objects_v2')
        objects = []
        prefixes = []

        for resp in paginator.paginate(Bucket=Bucket, Prefix=Prefix, Delimiter=Delimiter):
            objects.extend(resp.get('Contents', []))
            prefixes.extend(resp.get('CommonPrefixes', []))

        return {'Contents': objects, 'CommonPrefixes': prefixes}


    def list_objects_parallel(self, Prefix='/', Delimiter='/', Parallelism=None): #pylint: disable=invalid-name
        """
        credit: https://gist.github.com/sjakthol/a635e45b2da344b61db7d05bb6a0fbd3

        List objects of a bucket in parallel.
        The bucket must have a directory structure for speedups to be
        realized (each common prefix is listed in parallel).
        Args:
            :Bucket (str): the name of the bucket to list
            :Prefix (str): a prefix of the bucket to list (Default: None)
            :Delimiter (str): delimeter used to separate directories in S3 (Default: /)
            :Parallelism (int, optional): the number of threads to use (Default: 10xCPUs)
        Returns:
            obj: The list of objects under the given bucket / prefix::
                {
                    'Contents': [{
                        'Key': 'prefix/file.json',
                        'LastModified': datetime.datetime(2018, 12, 13, 14, 15, 16, 000000),
                        'ETag': '"58bcd9641b1176ea012b6377eb5ce050"'
                        'Size': 262756,
                        'StorageClass': 'STANDARD'
                    }]
                }
        """
        objects = []
        tasks = set()

        if not Parallelism:
            # Heavily oversubscribe the CPU as these operations are mostly bound to
            # network
            Parallelism = multiprocessing.cpu_count() * 10

        with concurrent.futures.ThreadPoolExecutor(max_workers=Parallelism) as tpe:
            tasks.add(
                tpe.submit(
                    self._list_objects_v2,
                    Bucket=self.bucket_name,
                    Prefix=Prefix,
                    Delimiter=Delimiter
                )
            )

            while tasks:
                done, _ = concurrent.futures.wait(tasks, return_when='FIRST_COMPLETED')
                for future in done:
                    res = future.result()
                    objects.extend(res['Contents'])

                    for prefix in res['CommonPrefixes']:
                        tasks.add(
                            tpe.submit(
                                self._list_objects_v2,
                                Bucket=self.bucket_name,
                                Prefix=prefix['Prefix'],
                                Delimiter=Delimiter
                            )
                        )

                tasks = tasks - done

        return {'Contents': objects}


    def download_s3_file(self, key, prefix_validation=True):
        """
        Get a file from S3 and cache it locally

        Args:
            key (str): Key of the object in the S3 bucket, must start with the base path of the class
        
        Returns:
            (tuple): file_path, file_extension
                where file_path is the relative path to the file from the cache directory

        """

        if prefix_validation and self.base_prefix not in key:
            raise ValueError(f'key must start with {self.base_prefix}')

        file_name = key.split('/')[-1]
        file_extension = file_name.split('.')[-1]

        file_dir = os.path.join(self.cache_dir, *key.split('/')[:-1])
        file_full_path = os.path.join(self.cache_dir, *key.split('/'))

        if os.path.exists(file_full_path):
            print(f'loading {file_full_path} from cache')
            return file_full_path, file_extension

        print(f'downloading {key} from S3')
        # create directory if it doesn't exist
        os.makedirs(file_dir, exist_ok=True)

        # download file
        s3 = boto3.client('s3', config=self.s3_config)
        s3.download_file(self.bucket_name, Key=key, Filename=file_full_path)

        return file_full_path, file_extension


    def get_s3_df(self, key, prefix_validation=True):
        """
        Get a DataFrame from S3, with caching, supporting CSV and Parquet

        Args:
            s3 (boto3.client): S3 client
            bucket_name (str): Name of the S3 bucket
            key (str): Key of the object in the S3 bucket
            cache_dir (str): Directory to cache the object locally

        Returns:
            pd.DataFrame: DataFrame from the S3 object

        Raises:
            ValueError: If the file format is not supported
        """
        
        cache_file, file_extension = self.download_s3_file(key, prefix_validation=prefix_validation)

        if file_extension == 'csv':
            return pd.read_csv(cache_file)

        if file_extension == 'parquet':
            return pd.read_parquet(cache_file)

        raise ValueError('Unsupported file format')


    def key_to_path(self, key):
        """
        Get the path of the object in the cache directory

        Args:
            key (str): Key of the object in the S3 bucket, must start with the base path of the class
        
        Returns:
            str: Relative path to the file from the cache directory
        """

        if self.base_prefix not in key:
            raise ValueError(f'key must start with {self.base_prefix}')

        return os.path.join(self.cache_dir, *key.split('/'))


    def _include_column(self, col):
        """ filters columns for the load profile DataFrame"""
        return (
            'electric' in col
            and 'intensity' not in col
            and 'total' not in col
            and 'net' not in col
            and 'pv' not in col
        )


    def _get_spatial_cols(self):

        if self.sim_year == '2021':
            select_cols = ['nhgis_county_gisjoin', 'resstock_county_id', 'state_abbreviation']
            join_col = 'nhgis_county_gisjoin'

        else:
            select_cols = ['nhgis_2010_county_gisjoin', 'county_name', 'state_abbreviation']
            join_col = 'nhgis_2010_county_gisjoin'

        return select_cols, join_col

   # ----------------------------------------------------------------------------------------------

    # api
    # ----------------------------------------------------------------------------------------------
    def get_spatial_tract_lookups(self, raw=False):
        """
            Get spatial tract lookups from S3

            Args:
                raw (bool): If True, returns the full DataFrame, else returns only relevant columns
            Returns:
                pd.DataFrame: DataFrame with spatial tract lookups
        """
        key = self.base_prefix + 'geographic_information/spatial_tract_lookup_table.csv'
        df = self.get_s3_df(key)

        if raw:
            return df

        if self.sim_geo_agg == 'by_county':
            county_cols, _ = self._get_spatial_cols()
            return (
                df
                [county_cols]
                .drop_duplicates()
                .reset_index(drop=True)
            )

        # by_state
        return (
            df
            [['state_abbreviation']]
            .drop_duplicates()
            .reset_index(drop=True)
        )


    def get_bldg_metadata(self):
        """
            Get building metadata from S3

            Args:
                cache_dir (str): Directory to cache the data
            
            Returns:
                pd.DataFrame: DataFrame with building metadata
        """

        # get list of keys from metadata folder
        folder = self.base_prefix + 'metadata/'
        _ls_res = self.list_objects_parallel(folder)
        keys = [obj['Key'] for obj in _ls_res['Contents']]

        # basecase: only has onefile
        for key in keys:
            if key.endswith('metadata.parquet'):
                return self.get_s3_df(key)

        if self.sim_upgrade == '0':
            suffix = 'baseline.parquet'
        else:
            suffix = f'upgrade_{self.sim_upgrade}.parquet'

        key = self.base_prefix + f'metadata/{suffix}'

        df = self.get_s3_df(key)

        # save extract
        _extract = {
            'in.sqft': 'int16',
            'in.geometry_building_type_recs': 'category',
            'in.bedrooms': 'int16',
            'in.county': 'category',
            'in.federal_poverty_level': 'category',
            'in.income': 'str',
            'in.occupants': 'str',
            'in.puma': 'category',
            'in.state': 'category',
            'in.tenure': 'category',
            'in.vacancy_status': 'category',
        }

        _income_maps = {
            '<10000':'<10k',
            '10000-14999':'10-15k',
            '15000-19999':'15-20k',
            '20000-24999':'20-25k',
            '25000-29999':'25-30k',
            '30000-34999':'30-35k',
            '35000-39999':'35-40k',
            '40000-44999':'40-45k',
            '45000-49999':'45-50k',
            '50000-59999':'50-60k',
            '60000-69999':'60-70k',
            '70000-79999':'70-80k',
            '80000-99999':'80-100k',
            '100000-119999':'100-120k',
            '120000-139999':'120-140k',
            '140000-159999':'140-160k',
            '160000-179999':'160-180k',
            '180000-199999':'180-200k',
            '200000+':'200k+',
        }

        _income_cats = [
            '<10k',
            '10-15k',
            '15-20k',
            '20-25k',
            '25-30k',
            '30-35k',
            '35-40k',
            '40-45k',
            '45-50k',
            '50-60k',
            '60-70k',
            '70-80k',
            '80-100k',
            '100-120k',
            '120-140k',
            '140-160k',
            '160-180k',
            '180-200k',
            '200k+',
        ]
        
        # _occupant_cats = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10+',]

        self._df_meta_extract = (
            df[_extract.keys()].astype(_extract)
            .rename(columns={col:col.split('.')[-1] for col in _extract.keys()})
            .assign(
                income = lambda x: x['income'].map(_income_maps),
            )
            .assign(
                income = lambda x: pd.Categorical(x['income'], ordered=True, categories=_income_cats),
                # occupants = lambda x: pd.Categorical(x['occupants'], ordered=True, categories=_occupant_cats)
                occupants = lambda x: np.where(x['occupants'] == '10+', '10', x['occupants']).astype('int16')
            )
        )

        return df


    @property
    def bldg_metadata_extract(self):
        if self._df_meta_extract is None:
            self.get_bldg_metadata()

        return self._df_meta_extract

    def get_sim_keys(self):
        """
            Get simulation keys from S3

            Args:
                s3 (boto3.client): S3 client
                prefix (str): Prefix to filter keys
            
            Returns:
                pd.DataFrame: DataFrame with simulation keys
        """
        PATH_NAME = os.path.join(
            self.key_to_path(self.base_prefix),
            '_staged',
            self.sim_level,
            self.sim_geo_agg,
            self.sim_upgrade,
            'sim_keys.parquet'
        )

        # found file locally
        if os.path.exists(PATH_NAME):
            print(f'loading {PATH_NAME} from cache')
            return pd.read_parquet(PATH_NAME)

        # get from S3
        print('scanning ', self.sim_prefix)
        _ls_res = self.list_objects_parallel(self.sim_prefix)
        df_keys = (
            pd.DataFrame.from_dict(_ls_res['Contents'])
            .pipe(lambda x: x.rename(columns={col:col.lower() for col in x.columns}))
            .assign(
                file=lambda x: x['key'].str.split('/').str[-1],
                size=lambda x: x['size'] / 1e6
            )
        )

        if self.sim_geo_agg == 'by_county':
            df_spatial = self.get_spatial_tract_lookups()
            _, join_col = self._get_spatial_cols()

            if self.sim_level == 'aggregate':
                county_func = lambda x: x['file'].str.split('-').str[0].str.upper()
            
            else:
                county_func = lambda x: x['key'].str.split('/').str[-2].str.split('=').str[-1]

            df_keys = (
                df_keys
                .assign(county=county_func)
                .merge(
                    df_spatial,
                    left_on='county',
                    right_on=join_col,
                    how='left'
                )
                .drop(columns=[join_col])
            )
        else:
            df_keys = (
                df_keys
                .assign(state=lambda x: x['key'].str.split('/').str[-2].str.split('=').str[-1])
            )

        # save to cache
        _path_head = os.path.split(PATH_NAME)[0]
        if not os.path.exists(_path_head):
            os.makedirs(_path_head)
        
        df_keys.to_parquet(PATH_NAME)

        return df_keys


    def get_weather_keys(self):
        PATH = os.path.join(
            self.key_to_path(self.base_prefix),
            '_staged',
            'weather_keys.parquet'
        )

        # found file locally
        if os.path.exists(PATH):
            print(f'loading {PATH} from cache')
            return pd.read_parquet(PATH)

        # get from S3
        _prefix = self.root + self.SIM_TREE[self.sim_year][self.sim_release]['weather_prefix']
        print('scanning ', _prefix)
        _ls_res = self.list_objects_parallel(_prefix)
        df_keys = (
            pd.DataFrame.from_dict(_ls_res['Contents'])
            .pipe(lambda x: x.rename(columns={col:col.lower() for col in x.columns}))
            .assign(
                file=lambda x: x['key'].str.split('/').str[-1],
                size=lambda x: x['size'] / 1e6
            )
        )

        # save to cache
        _path_head = os.path.split(PATH)[0]
        if not os.path.exists(_path_head):
            os.makedirs(_path_head)

        df_keys.to_parquet(PATH)

        return df_keys


    def get_weather_data(self, county_code):
        """
            Get weather data from S3

            Args:
                county_code (str): n code
            
            Returns:
                pd.DataFrame: DataFrame with weather data
        """
        WWATHER_COL_MAP = {
            'date_time':'date_time',
            'Dry Bulb Temperature [°C]':'temperature',
            'Relative Humidity [%]':'humidity',
            'Global Horizontal Radiation [W/m2]':'gh_irradiance',
        }
        weather_keys = self.get_weather_keys()
        key = weather_keys[weather_keys['key'].str.contains(county_code)]['key'].values[0]

        return (
            self.get_s3_df(key, prefix_validation=False)
            [WWATHER_COL_MAP.keys()]
            .rename(columns=WWATHER_COL_MAP)
        )


    def get_load_profile(self, key, include=None, exclude=None, add_weather=False, add_meta=False, add_Tfeatures=False):
        """
            Get a load profile DataFrame from S3

            Args:
                key (str): Key of the object in the S3 bucket
                include (list): List of columns to include, if None, include all
                exclude (list): List of columns to exclude, if None, exclude none

            Returns:
                pd.DataFrame: Load profile DataFrame
        """

        if self.sim_prefix not in key:
            raise ValueError(f'key must start with {self.sim_prefix}')

        if include:
            include += ['total']
        df = (
            self.get_s3_df(key)
            .set_index('timestamp')
            .pipe(lambda x: x[[col for col in x.columns if self._include_column(col)]])
            .assign(total = lambda x: x.sum(axis=1))
            .pipe(lambda x: x.rename(columns={
                col:(col.split('.')[-2] if '.' in col else col) for col in x.columns
            }))
            .pipe(lambda x: x[[col for col in x.columns if col not in exclude]] if exclude else x)
            .pipe(lambda x: x[[col for col in x.columns if col in include]] if include else x)
        )

        if add_Tfeatures:
            df = df.assign(
                month = lambda x: x.index.to_series().dt.month,
                day_of_week = lambda x: x.index.to_series().dt.dayofweek,
                hour = lambda x: x.index.to_series().dt.hour,
            )

        if add_weather or add_meta:
            bldg_id = key.split('/')[-1].split('-')[0]
            bldg_meta = self.bldg_metadata_extract.loc[int(bldg_id), :]

        if add_weather:
            # get county code
            county_code = bldg_meta['county']

            # get weather data
            df_weather = self.get_weather_data(county_code)

            # merge with load profile
            df = (
                df
                .pipe(lambda x: x.assign(join_dt=lambda y: y.index.to_series().dt.floor('h')))
                .reset_index()
                .merge(
                    df_weather.astype({'date_time': 'datetime64[ns]'}),
                    left_on='join_dt',
                    right_on='date_time',
                    how='left'
                )
                .bfill()
                .set_index('timestamp')
                .drop(columns = ['join_dt', 'date_time'])
            )

        if add_meta:
            df = df.assign(
                house__state = bldg_meta['state'],
                house__sqft = bldg_meta['sqft'],
                house__geometry = bldg_meta['geometry_building_type_recs'],
                house__bedrooms = bldg_meta['bedrooms'],
                house__occupants = bldg_meta['occupants'],
                house__tenure = bldg_meta['tenure'],
                house__vacancy = bldg_meta['vacancy_status'],
            )

        return df

    # ----------------------------------------------------------------------------------------------
    