import os
import glob
import re
import json
import sys
import tqdm
import pandas as pd
sys.path.append("/Users/jammyc/Desktop/classwork/UCB-MIDS/DATASCI-210/project/capstone/src")
from botocore import UNSIGNED
from botocore.config import Config
from resloader.base import ResLoader
from datetime import datetime
import pdb

class dataLoader:
	def __init__(self, params):
		self.target_columns = [
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

		self.resloader = ResLoader(
		    cache_dir=params['cache_dir'],
		    s3_config=Config(signature_version=UNSIGNED),
		    sim_year=params['year'],
		    sim_release=params['release'],
		    sim_upgrade=params['upgrade'],
		    sim_level=params['level'],
		    sim_geo_agg=params['aggregate']
		)

		self.params = params
		self.building_keys = self.resloader.get_sim_keys()
		self.df_spatial_tract_lookups = self.resloader.get_spatial_tract_lookups()
		self.building_data = {'params' : self.params, 's3_data' : {}}
		self.dataroot = "./traindata/"

	def save_data_to_parquet(self, df, building_id):
		outdir = f"{self.dataroot}{building_id}"
		os.makedirs(outdir, mode = 0o755, exist_ok = True) 
		df.to_parquet(f"{self.dataroot}{building_id}/{building_id}.parquet", engine='pyarrow')

	def load_samples(self):
		pbar = tqdm.tqdm(total=len(self.building_keys))
		for idx, row in self.building_keys.iterrows():
			mat = re.search(r'(\d+)\-(\d+)\.parquet', row['key'])		
			if mat:
				building_id = mat.group(1)
			else:
				print(f"Unknown building_id {row['key']}")
				continue
			# skip S3 data loading if building_id dataframe already available
			if os.path.exists(f"{self.dataroot}{building_id}/{building_id}.parquet"):
		         	continue	
			try:
				#self.building_data['s3_data'][building_id] = pd.read_parquet(self.params['cache_dir'] + row['key'], engine='pyarrow')
				df = pd.read_parquet(self.params['cache_dir'] + row['key'], engine='pyarrow')
#				print(f"Loaded from cache: {self.params['cache_dir'] + row['key']}")
			except:
#				print(f"Loading from S3: {self.params['cache_dir'] + row['key']}")
				#self.building_data['s3_data'][building_id] = self.resloader.get_load_profile(row['key'], include=self.target_columns)
				df = self.resloader.get_load_profile(row['key'], include=self.target_columns)
			pbar.set_description(f"Saving data: {building_id}")
			self.save_data_to_parquet(df, building_id)
			pbar.update(1)

	def load_traindata(self, num_houses):
		datafiles = glob.glob(f"{self.dataroot}*/*.parquet", recursive=True)
		building_df = pd.DataFrame([])
		for df in datafiles:
			if num_houses > 0 or num_houses == -1:
				mat = re.search(r'(\d+)\.parquet$', df)
				if mat:
					bdata = pd.read_parquet(df, engine='pyarrow')
					bdata['building_id'] = mat.group(1)
					bdata['year'] = bdata.index.year
					bdata['month'] = bdata.index.month
					bdata['date'] = bdata.index.day
					if building_df.empty:
						building_df = bdata
					else:
						building_df = pd.concat([building_df, bdata], axis=0)
				else:
					print(f"Unable to find building ID parquet file {df}")
				if num_houses != -1:
					num_houses -= 1
			else:
				return building_df
		
	def split_train_test(self, num_houses=-1):
		print(f"Loading [{num_houses}] houses data...")
		df = self.load_traindata(num_houses)
#		for entry in df.iterrows():
#			pdb.set_trace()
		for month in range(1,13):
			# split first 3 weeks of each month for train
			# last week of each month for test
			# for each month, take date 1st to 21st
			traindata.loc[(traindata.index.day >= 1) & (traindata.index < end_date)]
				
		


#start_date = datetime(2018, 1, 1)
#end_date = datetime(2018, 1, 22)


# first 3 weeks of every month for training, last week for validation.

# setup
config = {
		'cache_dir' : './s3_cache', 
		'year' : '2021', 
		'release' : 'resstock_amy2018_release_1', 
	  	'level' : 'individual', 
		'aggregate' : 'by_county', 
		'upgrade' : '0'
	}

dl = dataLoader(config)
dl.load_samples()
#traindata = dl.split_train_test(100)
pdb.set_trace()
print(traindata)
