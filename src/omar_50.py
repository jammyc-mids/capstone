import sys
import os
sys.path.append('path/to/src')

# import pandas as pd
import numpy as np

from botocore import UNSIGNED
from botocore.config import Config

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
# import seaborn as sns
# import missingno as msno

from resloader.base import ResLoader

# from sklearn.preprocessing import OneHotEncoder

CACHE_DIR = 'path/to/cache'

loader = ResLoader(
    cache_dir=CACHE_DIR,
    s3_config=Config(signature_version=UNSIGNED),
    sim_year='2022',
    sim_release='resstock_amy2018_release_1.1',
    sim_upgrade='0',
    sim_level='individual',
    sim_geo_agg='by_state',
)

sim_keys = loader.get_sim_keys()

sample_keys = sim_keys.sample(150, random_state=42)['key'].values

for key in sample_keys:
    sample = loader.get_load_profile(key)