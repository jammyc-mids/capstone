from resloader import api as resloader_api

# get spatial tract lookups from S3
spatial_tract_lookups = resloader_api.get_spatial_tract_lookups()
print(spatial_tract_lookups.head())
print()

# get individual building simulation keys from S3
# takes a while to run (~2mins with 30 workers)
individual_bldg_sim_keys = resloader_api.get_individual_bldg_sim_keys(num_workers=20)
print(individual_bldg_sim_keys.head())
print()

# get sample building
sample_building = individual_bldg_sim_keys['key'].sample(1).values[0]
sample_building_df_1 = resloader_api.get_load_profile(sample_building)
print(sample_building_df_1.head())

# hint: look at resloader_api docstring you can select the columns you want to load