from botocore import UNSIGNED
from botocore.config import Config
from resloader.base import ResLoader

# setup
CACHE_DIR = '/Users/darwish/Documents/Berkeley_Offline/W210/capstone/s3_cache'
TARGET_COLS = [
    'ceiling_fan',
    'clothes_dryer',
    'clothes_washer',
    'cooking_range',
    'cooling',
    'heating',
    'dishwasher',
    'ext_lighting',
    'interior_lighting',
    'refrigerator',
    'freezer'
]

loader = ResLoader(
    cache_dir=CACHE_DIR,
    s3_config=Config(signature_version=UNSIGNED),
    sim_year='2021',
    sim_release='resstock_amy2018_release_1',
    sim_upgrade='0',
    sim_level='individual',
    sim_geo_agg='by_county',
)

# load spatial tract
df_spatial_tract_lookups = loader.get_spatial_tract_lookups()
print(df_spatial_tract_lookups.head(), end='\n\n')

# load simulation keys
df_sim_keys = loader.get_sim_keys()
print(df_sim_keys.head(), end='\n\n')

# load sample
sample_key = df_sim_keys['key'].sample(1).values[0]
df_sample_load = loader.get_load_profile(sample_key, include=TARGET_COLS)
print(df_sample_load.head(), end='\n\n')
