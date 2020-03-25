#!/usr/bin/env python
# coding: utf-8

# ### John Hopkins GitHub Repo Data Wrangling

# In[32]:


import json
import os
import sys
import pandas as pd
import numpy as np
import boto3
import uuid
from slugify import slugify


# In[33]:


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__name__))), 'COVID-19')


# In[34]:


# the confirmed cases time series files in here have Lat and Long for each location
confirmed_series_file = os.path.join(
    DATA_DIR,
    'csse_covid_19_data',
    'csse_covid_19_time_series',
    'time_series_19-covid-Confirmed.csv'
)
confirmed_series_file


# In[35]:


# show first 10 rows of time_series_19-covid-Confirmed.csv file
#!awk -F, '{print $1,$2,$3,$4} NR==10{exit}' OFS=', ' \
# COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv


# In[36]:


cols=['Province/State', 'Country/Region', 'Lat', 'Long']
locations_df = pd.read_csv(confirmed_series_file, usecols=cols)
locations_df.head()


# In[37]:


s3_resource = boto3.resource('s3')
bucket_name = 'thecodinginterface-covid'
s3_bucket = s3_resource.Bucket(name=bucket_name)


# In[38]:


def slugify_location(country_region, province_state):
    if province_state:
        return slugify(f"{country_region}-{province_state}")
    return slugify(country_region)

def cloud_resource_url(filename, bucket_name):
    return f"https://{bucket_name}.s3.amazonaws.com/{filename}.json"

def upload_file_to_s3(s3_bucket, file_path, file_name):
    s3_bucket.upload_file(
        Filename=file_path,
        Key=file_name,
        ExtraArgs={'ACL':'public-read'}
    )
    return cloud_resource_url(file_name, s3_bucket.name)


# In[39]:


# rename columns to be snake_cased making it more ammenable to serialization
locations_df = locations_df.rename(columns={
    'Province/State': 'province_state',
    'Country/Region': 'country_region',
    'Lat': 'lat',
    'Long': 'long'
})

# make sure text columns are well cleaned and stripped of whitespace
locations_df.province_state = locations_df.province_state.str.strip()
locations_df.country_region = locations_df.country_region.str.strip()

# Fill NaNs with empty strings in the Province/State columns because this data will
# be serialized into JSON which does not support NaN
locations_df.province_state = locations_df.province_state.fillna('')

# create columns "filename" and "cloud_resource"
lookup_keys = zip(locations_df.country_region, locations_df.province_state)
locations_df['location_id'] = [slugify_location(country_region, province_state)
                            for country_region, province_state in lookup_keys]

locations_df['cloud_resource'] = [cloud_resource_url(filename, bucket_name)
                                  for filename in locations_df['location_id'].values]

locations_df.head()


# In[40]:


locations_df = locations_df.set_index('location_id')
locations_df[locations_df.country_region == 'US'].sort_values('province_state')


# In[41]:


# I'll do more with this locations_df DataFrame later after
# constructing country specific case data sets


# In[42]:


# build list of daily csv files
confirmed_series_dir = os.path.join(
    DATA_DIR,
    'csse_covid_19_data',
    'csse_covid_19_daily_reports'
)
daily_csv_files = [file_name
                   for file_name in os.listdir(confirmed_series_dir) 
                   if file_name.endswith('csv')]
daily_csv_files[:5]


# In[43]:


# take a peek at the structure of a file that will be worked with
os.path.join(confirmed_series_dir, daily_csv_files[0])


# In[44]:


#!head ./COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/02-26-2020.csv


# In[45]:


def calc_differential(x):
    x0 = np.array([0] + x[:-1].tolist())
    dx = x.values - x0
    return dx


# In[46]:


# read the daily files into DataFrame objects then concatenate them together
daily_dfs = []
colunns_of_interest = [
    'province_state',
    'country_region',
    'total_confirmed',
    'total_deaths',
    'total_recovered',
    'date'
]

for file_name in daily_csv_files:
    file_path = os.path.join(confirmed_series_dir, file_name)
    day_df = pd.read_csv(file_path)

    # Province_State and Country_Region replaced column names Province/State
    # and Country/Region for new daily files starting 03-24-2020
    day_df = day_df.rename(columns={
        'Province/State': 'province_state',
        'Province_State': 'province_state', 
        'Country/Region': 'country_region',
        'Country_Region': 'country_region',
        'Confirmed': 'total_confirmed',
        'Deaths':'total_deaths',
        'Recovered': 'total_recovered'
    })
    
    date_str, ext = os.path.splitext(file_name)
    num_rows = day_df.shape[0]
    day_df['date'] = [pd.to_datetime(date_str)] * num_rows
    
    missing_columns = sum([(col not in day_df.columns) for col in colunns_of_interest])
    if missing_columns:
        import pdb; pdb.set_trace()
        sys.exit(0)
        
    # increased granularity by neighborhood was added in Admin2 column 03-24-2020
    # but only want granularity down to province_region so collapse down and aggregate
    day_df = day_df[colunns_of_interest]
    day_df = day_df.groupby(['country_region', 'province_state', 'date']).sum()
    day_df = day_df.reset_index()
    
    daily_dfs.append(day_df[colunns_of_interest])
    
daily_df = pd.concat(daily_dfs)

# Fill NaNs with empty strings because this data will
# be serialized into JSON which does not support NaN
daily_df.province_state = daily_df.province_state.fillna('')
daily_df.total_confirmed = daily_df.total_confirmed.fillna(0)
daily_df.total_deaths = daily_df.total_deaths.fillna(0)
daily_df.total_recovered = daily_df.total_recovered.fillna(0)

# make sure text columns are well cleaned and stripped of whitespace
daily_df.province_state = daily_df.province_state.str.strip()
daily_df.country_region = daily_df.country_region.str.strip()
    
locations = zip(daily_df.country_region.values, daily_df.province_state.values)
daily_df['location_id'] = [slugify_location(country_region, province_state)
                           for country_region, province_state in locations]

# sort by country_region, province_state, date
daily_df = daily_df.sort_values(['country_region', 'province_state', 'date'])
daily_df.head()


# In[47]:


# get totals per location
max_date = daily_df.date.max()
rows_of_interest = daily_df.date == max_date
columns_of_interest = [
    'location_id',
    'total_confirmed',
    'total_deaths',
    'total_recovered'
]
location_totals_df = daily_df.loc[rows_of_interest, columns_of_interest]
location_totals_df = location_totals_df.groupby('location_id').sum()
location_totals_df['death_rate'] = location_totals_df.total_deaths / location_totals_df.total_confirmed * 100
location_totals_df['recovery_rate'] = location_totals_df.total_recovered / location_totals_df.total_confirmed * 100
location_totals_df.sort_values('total_confirmed', ascending=False).head(10)


# In[48]:


# get totals per country / region
country_totals_df = locations_df.join(location_totals_df)
columns_of_interest = [
    'country_region',
    'total_confirmed',
    'total_deaths',
    'total_recovered',
]
country_totals_df = country_totals_df.reset_index()
country_totals_df = country_totals_df[columns_of_interest].groupby('country_region').sum()
country_totals_df['death_rate'] = country_totals_df.total_deaths / country_totals_df.total_confirmed * 100
country_totals_df['recovery_rate'] = country_totals_df.total_recovered / country_totals_df.total_confirmed * 100
country_totals_df.sort_values('country_region').head(25)


# In[49]:


world_population_df = pd.read_csv('world_population.csv')
world_population_df = world_population_df.set_index('country_region')
world_population_df.head()


# In[50]:


# add population data to country totals
country_totals_df = country_totals_df.join(world_population_df)
country_totals_df.head(20)


# In[51]:


daily_df.head()


# In[52]:


# group by location and serialize each location dataset to a json file
# [ 
#   {
#     date: str,
#     province_state: str,
#     confirmed: int,
#     deaths: int,
#     recovered: int
#   }, ...
# ]

location_case_data = 'location_case_data'
if not os.path.exists(location_case_data):
    os.mkdir(location_case_data)

location_groups = daily_df.groupby(['location_id'])
for location_id, location_data in location_groups:
    location_data.loc[:,'daily_confirmed'] = calc_differential(location_data.total_confirmed)
    location_data.loc[:,'daily_deaths'] = calc_differential(location_data.total_deaths)
    location_data.loc[:,'daily_recovered'] = calc_differential(location_data.total_recovered)
    
    location_days = []

    for idx, row in location_data.iterrows():
        data = row.to_dict()
        # dates don't serialize well in Python so, convert to strings
        data['date'] = data['date'].strftime('%Y-%m-%d')
        location_days.append(data)

    filename = f"{location_id}.json"
    file_path = os.path.join(location_case_data, filename)

    with open(file_path, 'w') as fo:
        json.dump(location_days, fo, indent=4)

    s3_url = upload_file_to_s3(s3_bucket, file_path, filename)


# In[53]:


locations_df.head(25)
locations_df = locations_df.reset_index()
locations_df.head()


# In[54]:


# create a list of dicts in the form:
# [ 
#   {
#     country_region: str,
#     province_state: str,
#     lat: float,
#     long: float,
#     filename: str,
#     cloud_resource: str
#   },
#    ...
# ]
locations = []
location_groups = locations_df.groupby(['location_id'])
for k, location_data in location_groups:
    for i, row in location_data.iterrows():
        data = row.to_dict()
        locations.append(data)
        if i < 5:
            print(data)


# In[55]:


# serialize locations to JSON file
with open('locations.json', 'w') as fo:
    json.dump(locations, fo, indent=4)

get_ipython().system('head -n 15 locations.json')


# In[ ]:





# ## Country / Region Dashboard
# 
# Give user ability to select (aka drill down) into country, region, state, province
# 
# Show confirmed, deaths, recovered
# 
# Show time series of total confirmed
# 
# Show time series of total recovered
# 
# Show time series of total deaths
# 
# Show time series of daily new confirmed
# 
# Show time series of daily new recovered
# 
# Show time series of daily new deaths
# 
# Would be interesting to give a Gauge chart next to the daily graphs with an indicator of direction of n day movement (ie, over the last three days is new daily cases (deaths, confirmed, recovered) increasing, descreasing, maintaining)
# 
# 
# ## Location Comparisons
# 
# ### Barcharts
# 
# Death Rates: select locations to include and date in time (includes checkbox to make percent of population)
# 
# Confirmed Counts: select locations to include and date in time (includes checkbox to make percent of population)
# 
# 
# ### Line Charts
# 
# Total Confirmed Cases: select locations to include and plot progression of cases since first case (includes checkbox to make percent of population)
# 
# New Daily Confirmed Cases: select locations to include and plot progression of new cases since first case in each location (includes checkbox to make percent of population)
# 
# Total Deaths: select locations to include and plot progression of deaths since first case (includes checkbox to make percent of population)
# 
# New Daily Deaths: select locations to include and plot progression of deaths since first  case in each location (includes checkbox to make percent of population)

# In[23]:


locations_df.to_csv('locations.csv')


# In[ ]:




