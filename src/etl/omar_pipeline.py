import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


import pyspark.sql.functions as F
import pyspark.sql.types as T
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf_profiles = glueContext.create_dynamic_frame.from_catalog(database='resstock_test', table_name='profiles')
dyf_metadata = glueContext.create_dynamic_frame.from_catalog(database='resstock_test', table_name='metadata')
PROFILE_MAPPING = [
    ('timestamp', 'timestamp', 'timestamp', 'timestamp'),
    ('bldg_id', 'long', 'k__bldg_id', 'long'),
    ('`out.outdoor_air_dryblub_temp.c`', 'double', 'x__temperature', 'double'),
    ('`out.electricity.clothes_dryer.energy_consumption`', 'double', 'y__clothes_dryer', 'double'),
    ('`out.electricity.clothes_washer.energy_consumption`', 'double', 'y__clothes_washer', 'double'),
    ('`out.electricity.cooling_fans_pumps.energy_consumption`', 'double', 'y__cooling_fans_pumps', 'double'),
    ('`out.electricity.cooling.energy_consumption`', 'double', 'y__cooling', 'double'),
    ('`out.electricity.dishwasher.energy_consumption`', 'double', 'y__dishwasher', 'double'),
    ('`out.electricity.freezer.energy_consumption`', 'double', 'y__freezer', 'double'),
    ('`out.electricity.heating_fans_pumps.energy_consumption`', 'double', 'y__heating_fans_pumps', 'double'),
    ('`out.electricity.heating.energy_consumption`', 'double', 'y__heating', 'double'),
    ('`out.electricity.hot_water.energy_consumption`', 'double', 'y__hot_water', 'double'),
    ('`out.electricity.lighting_exterior.energy_consumption`', 'double', 'y__lighting_exterior', 'double'),
    ('`out.electricity.lighting_interior.energy_consumption`', 'double', 'y__lighting_interior', 'double'),
    ('`out.electricity.range_oven.energy_consumption`', 'double', 'y__range_oven', 'double'),
    ('`out.electricity.refrigerator.energy_consumption`', 'double', 'y__refrigerator', 'double'),
]

METADATA_MAPPING = [
    ('bldg_id', 'long', 'k__bldg_id_2', 'long'),
    ('`in.sqft`', 'double', 'h__sqft', 'double'),
    ('`in.state`', 'string', 'h__state', 'string'),
    ('`in.geometry_building_type_recs`', 'string', 'h__geometry_building_type_recs', 'string'),
    ('`in.bedrooms`', 'string', 'h__bedrooms', 'long'),
    ('`in.occupants`', 'string', 'h__occupants', 'string'),
    ('`in.vacancy_status`', 'string', 'h__vacancy_status', 'string'),
    ('`in.tenure`', 'string', 'h__tenure', 'string'),
]

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
    ['k__bldg_id', 't__month', 't__day_of_week', 't__day_of_month']
    + ['h__state', 'h__geometry_building_type_recs', 'h__bedrooms', 'h__occupants', 'h__sqft', 'h__is_occupied', 'h__is_owner']
    + [f'{col}__{str(t).zfill(2)}' for col in ('x__aggregate', 'x__temperature') for t in range(96)]
    + [f'{col}__{str(t).zfill(2)}' for col in LOAD_LIST for t in range(96)]
) 
df_metadata = (
    dyf_metadata
    .apply_mapping(METADATA_MAPPING)
    .toDF()
    # convert '10+' occupants to 10
    .withColumn('h__occupants', F.regexp_extract(F.col('h__occupants'), '(\d+)', 1).cast(T.IntegerType()))
    
    # convert vacancy status to binary indicator
    .withColumn('h__is_occupied', F.when(F.col('h__vacancy_status') == 'Occupied', 1).otherwise(0))
    # convert tenure to binary indicator
    .withColumn('h__is_owner', F.when(F.col('h__tenure') == 'Owner', 1).otherwise(0))
    .drop('h__vacancy_status', 'h__tenure')
)
df_profiles = (
    dyf_profiles
    .apply_mapping(PROFILE_MAPPING)
    .toDF()
)

# assume 2018-01-01 00:00:00 is a duplicate of 2018-01-01 00:15:00
start_timestamp = F.unix_timestamp(F.lit('2018-01-01 00:15'), 'yyyy-MM-dd HH:mm').cast('timestamp')
desired_start_timestamp = F.unix_timestamp(F.lit('2018-01-01 00:00'), 'yyyy-MM-dd HH:mm').cast('timestamp')
first_rows = df_profiles.filter(F.col('timestamp') == start_timestamp)
adjusted_rows = first_rows.withColumn('timestamp', F.lit(desired_start_timestamp))

df_profiles = (
    df_profiles
    # remove 2019 entry for all houses 2019-01-01 00:00:00
    .filter(F.year(F.col('timestamp')) != 2019)
    
    # add synthetic 2018-01-01 00:00:00 entry for all houses
    .unionByName(adjusted_rows)
    
    # combine cooling and cooling_fans_pumps
    .withColumn('y__cooling', F.round(F.col('y__cooling') + F.col('y__cooling_fans_pumps'), 3))
    
    # combine heating and heating_fans_pumps
    .withColumn('y__heating', F.round(F.col('y__cooling') + F.col('y__heating_fans_pumps'), 3))
    
    # calculate agrregate
    .withColumn('x__aggregate', F.round(sum([F.col(x) for x in LOAD_LIST]), 3))
    
    # calculate daily sequence value (0-95)
    .withColumn('k__seq', (F.hour('timestamp') * 4) + (F.minute('timestamp') / 15).cast('int'))
    .withColumn('k__seq', F.lpad(F.col('k__seq').cast('string'), 2, '0'))
    
    # calcualte t-features
    .withColumn('t__day_of_week', F.dayofweek('timestamp'))
    .withColumn('t__day_of_month', F.dayofmonth('timestamp'))
    .withColumn('t__month', F.month('timestamp'))
    
    # drop unnecessary columns
    .drop('timestamp', 'y__cooling_fans_pumps', 'y__heating_fans_pumps')
)
# pivot profiles data frame
id_cols = sorted(['k__bldg_id', 'k__seq', 't__day_of_week', 't__day_of_month', 't__month'])
melt_cols = sorted(list(set(df_profiles.columns) - set(id_cols)))

_sub = ', '.join([f'\'{col}\', {col}' for col in melt_cols])
query = f"stack({len(melt_cols)}, {_sub})" 

df_pivoted = (
    df_profiles
    # melt dataframe
    .select(id_cols + [F.expr(query).alias('load_id', 'value')])
    
    # define pivot column
    .withColumn('k__pivot_id', F.concat_ws('__', 'load_id', 'k__seq'))
    
    # group by house, day (and covariates)
    .groupby(*list(set(id_cols) - set(['k__seq'])))
    
    # pivot dataframe
    .pivot('k__pivot_id')
    .agg(F.first('value'))
)
# join metadata and re-order columns
df_out = (
    df_pivoted
    .join(df_metadata, df_pivoted['k__bldg_id'] == df_metadata['k__bldg_id_2'], 'left')
    .select(OUT_COLS)
)
# convert back to amazon class
dyf_out = DynamicFrame.fromDF(df_out, glueContext, 'dyf_out')
# send to s3
s3output = glueContext.getSink(
    connection_type='s3',
    path='s3://et-resstock-test-subset/outputs/001/',
    partitionKeys=[],
    compression='snappy',
)
s3output.setFormat('glueparquet')
s3output.writeFrame(dyf_out)
job.commit()