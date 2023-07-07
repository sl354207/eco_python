# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import json
import glob
import os
import geopandas as gpd
from dask import dataframe as dd
import dask_geopandas
from shapely import wkt
import vaex
import time

from pyinaturalist import (
    Taxon,
    enable_logging,
    get_taxa,
    get_taxa_autocomplete,
    get_taxa_by_id,
    pprint,
)

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)




# %%
# WWF ECOREGION MAP DATA

# read in wwf map data
ej = pd.read_json(
    '/home/muskrat/Documents/eco_data/wwf_map_data/wwf_terr_ecos.json')
# ej = pd.read_json(
#     '/media/muskrat/060A9B8E0A9B78FF/map_final/eco_map.geojson')
# print(ej['features'][0])
# %%
# WWF ECOREGION MAP DATA

# unpack json file and convert to dataframe


def unpack(df, column, fillna=None):
    ret = None
    if fillna is None:
        tmp = pd.DataFrame((d for idx, d in df[column].iteritems()))
        ret = pd.concat([df.drop(column, axis=1), tmp], axis=1)
    else:
        tmp = pd.DataFrame((d for idx, d in
                            df[column].iteritems())).fillna(fillna)
        ret = pd.concat([df.drop(column, axis=1), tmp], axis=1)
    return ret


ej1 = unpack(ej, 'features', 0)

ej2 = unpack(ej1, 'properties', 0)

# ej2.drop_duplicates(subset=['unique_id'], keep='first', inplace=True)
# ej2.reset_index(drop=True, inplace=True)

# ej3 = unpack(ej2, 'geometry', 0)

# ej3.dropna(subset=['unique_id'], inplace=True)
# ej3.reset_index(drop=True, inplace=True)

# # df = pd.DataFrame(ej3.coordinates[0])

# # df1 = df[0][0]

# selected = []

# for i in range(len(ej2)):
#     # df2 = pd.DataFrame(ej3.coordinates[i])
#     # df3 = df2[0][0]
#     df3 = ej2.geometry[i]['coordinates'][-1][-1]
#     if len(df3) > 2:
#         selected.append(df3[-1])
#     else:
#         selected.append(df3)
# # ej4 = ej3['coordinates'][0][0][0]

# import math

# desk = []

# for i in range(len(selected)):
#     d = selected[i]

#     deesk = []
#     for j in range(len(d)):
#         factor = 10.0 ** 3
#         t = math.trunc(d[j] * factor) / factor
#         deesk.append(t)
#     desk.append(deesk)

# ej2['point'] = desk
# ej2.rename(columns={'point': 'coordinates'}, inplace=True)

# ecoregions = ej2[['unique_id', 'name', 'TYPE', 'coordinates']]

# ej3.iloc[[0], []] = ej4


# create subset of dataframe just to check data
# ej3 = ej2[['OBJECTID', 'ECO_NAME', 'ECO_NUM', 'ECO_ID', 'eco_code']]

# rename eco_code column
ej2.rename(columns={'eco_code': 'ECOREGION_CODE'}, inplace=True)
ej2.drop(ej2.columns[0], axis=1, inplace=True)
ej2.drop(ej2.columns[0], axis=1, inplace=True)

# ej2.insert(0, 'type', 'Feature')

# create subset of dataframe
ej5 = ecoregions[['unique_id', 'ECOREGION_CODE']]

ej4 = pd.merge(ej2, ej5, on='ECOREGION_CODE', how='left')


ej6 = ej4.to_dict(orient='records')

# ej6 = ej2.to_dict(orient='records')
# add properties wrapper key to values

k = "properties"

fruit_dictionary = [{k: fruit} for fruit in ej6]

# convert to json
fruitj = json.dumps(fruit_dictionary)

fruitd = pd.DataFrame(fruit_dictionary)

ej1.drop(ej1.columns[0], axis=1, inplace=True)
ej1.drop(ej1.columns[1], axis=1, inplace=True)

ej1.insert(0, 'type', 'Feature')
# ej1.insert(0, 'type', 'Feature')

# join dataframes

mapd = pd.concat([ej1, fruitd], axis=1, join='inner')
mapj = mapd.to_json(orient='records', force_ascii=False)
# eco = ecoregions.to_json(orient='records', force_ascii=False)

# mapj = fruitd.to_json(orient='records', force_ascii=False)
repr(mapj)

# add start and end tag to json

mapf = '{"type":"FeatureCollection", "features": ' + mapj + '}'


# %%
# WWF ECOREGION MAP DATA

# write to json file
file = open("dataset.json", "w")
file.write(mapf)
file.close

# file = open("eco.json", "w")
# file.write(eco)
# file.close

# %%
# WWF ECOREGION AND USA COUNTY MAP DATA

# read in ecoregion map data and US county data
counties = gpd.read_file(
    '/home/muskrat/Documents/eco_data_copy/wwf_map_data/us_counties.geojson')
ecomap = gpd.read_file(
    '/home/muskrat/Documents/eco_data_copy/wwf_map_data/map.geojson')

# ecotest = ecomap.loc[ecomap['unique_id'] == 420].reset_index(drop=True)
# county_inner = counties.loc[0]
# county_outer = counties.loc[1]
# county_split = counties.loc[(counties['NAME'] == 'Brown') & (counties['STATE_NAME'] == 'Ohio')]


eco_county = []
# eco_id = {'unique_id': str(ecotest['unique_id'].values[0])}
# eco_county.append(eco_id)
# selected_counties = []

# check whether a each county is inside or intersects an ecoregion or if ecoregion is inside a county
for i in range(len(ecomap)):
    # get unique id of each ecoregion
    eco_id = {'unique_id': str(ecomap.loc[i, 'unique_id'])}

    selected_counties = []

    for j in range(len(counties)):
        # check whether county is within ecoregion
        # contains = ecomap.loc[i, 'geometry'].contains(counties.loc[j, 'geometry'])
        # intersects = ecomap.loc[i, 'geometry'].intersects(counties.loc[j, 'geometry'])
        within = ecomap.loc[i, 'geometry'].within(counties.loc[j, 'geometry'])

        # print(counties.loc[i, 'NAME'])
        # if county is in ecoregion push to selected counties list
        # if contains == True:
        if within == True:
            # if contains == True or intersects == True:
            selected_counties.append({
                'county': counties.loc[j, 'NAME'],
                'state': counties.loc[j, 'STATE_NAME']})
            # eco_id['eco_counties'] = selected_counties
       # add selected counties to eco_id dict
    eco_id['eco_counties'] = selected_counties
    # push counties to eco_county list
    eco_county.append(eco_id)


full_county = []

# check if there are any counties in ecoregion for usa. if they are push to full county list
for i in range(len(eco_county)):
    if eco_county[i]['eco_counties']:
        full_county.append(eco_county[i])

na_county = []

# check if ecoregion has unique_id. if it does push to na_county
for i in range(len(full_county)):
    if full_county[i]['unique_id'] != 'nan':
        na_county.append(full_county[i])

# convert to dataframe
nd = pd.DataFrame(na_county)
nd1 = pd.json_normalize(na_county,
                        record_path='eco_counties',
                        meta=['unique_id'])

# add columns
nd2 = nd1[['unique_id', 'county']]
nd3 = nd1[['unique_id', 'state']]

# group by ecoregion
group = nd2.groupby('unique_id')['county'].apply(list).reset_index()

group1 = nd3.groupby('unique_id')['state'].apply(list).reset_index()

final_eco_county = pd.merge(group, group1, on='unique_id', how='left')

nd1j = nd1.to_json(orient='records', force_ascii=False)
repr(nd1j)

file = open("eco_county_prep.json", "w")
file.write(nd1j)
file.close

# %%
# USA ECO COUNTIES

eco_county = pd.read_json('/home/muskrat/Desktop/eco_county_prep.json')

# convert to dataframe and group county and state into dict.
county = ['county', 'state']
eco_county['eco_counties'] = eco_county[county].to_dict(orient='records')

# group by unique id
eco_group = eco_county.groupby(
    'unique_id')['eco_counties'].apply(list).reset_index()
# eco_group = eco_county.groupby('unique_id')['eco_counties']

# convert to list
eco_counties = eco_group['eco_counties'].to_list()

eco_counties_test = eco_counties

# remove duplicates from list of counties and states
for j in range(len(eco_counties_test)):

    eco_counties_test[j] = [i for n, i in enumerate(
        eco_counties_test[j]) if i not in eco_counties_test[j][n + 1:]]


eco_counties_df = pd.Series(eco_counties_test, name='counties_final')

group_final = eco_group.merge(eco_counties_df, left_index=True, right_index=True).reindex(
    columns=['unique_id', 'counties_final'])

group_finalj = group_final.to_json(orient='records', force_ascii=False)
repr(group_finalj)

file = open("eco_county_within.json", "w")
file.write(group_finalj)
file.close

# j_test = pd.read_json('/home/muskrat/Desktop/eco_county.json')

# %%
# USA ECO COUNTIES

eco_county_contains = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/eco_county_contains.json')
eco_county_intersects = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/eco_county_intersects.json')
eco_county_within = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/eco_county_within.json')


# %%
# USA ECO COUNTIES

eco_ods = pd.read_excel('/home/muskrat/Documents/eco_data_copy/US_county_plants/397/397.ods',
                        index_col=0, sheet_name=None, engine=('odf'))

# put all sheets in one dataframe
eco_df = pd.concat([v for k, v in eco_ods.items()])
eco_df.reset_index(inplace=True)

# add unique id column with ecoregion value
eco_df['unique_id'] = '397'

# %%
# USA ECO COUNTIES
# remove duplicate species
eco_df.drop_duplicates(['Scientific Name'], inplace=True, ignore_index=True)

# %%
# USA ECO COUNTIES

eco_f = eco_df[['Plant Type', 'Scientific Name', 'Common Name',
                'Plant Family', 'Native Status', 'unique_id']]

eco_j = eco_f.to_json(orient='records', force_ascii=False)
repr(eco_j)

file = open("eco_county_397.json", "w")
file.write(eco_j)
file.close

# j_test = pd.read_json('/home/muskrat/Documents/eco_data_copy/US_county_plants/397/eco_county_397.json')

# %%
# USA ECO COUNTIES

# read in all json files and merge into one dataframe
json_dir = '/home/muskrat/Documents/eco_data_copy/US_county_plants/eco_json'

json_pattern = os.path.join(json_dir, '*.json')
file_list = glob.glob(json_pattern)

dfs = []
for file in file_list:
    with open(file) as f:
        json_data = pd.json_normalize(json.loads(f.read()))
        json_data['site'] = file.rsplit("/", 1)[-1]
    dfs.append(json_data)
counties_init = pd.concat(dfs)
counties_init.reset_index(drop=True, inplace=True)
counties_init.drop('site', 1, inplace=True)

# %%

counties_init['Plant Type'].replace(
    to_replace='Tree, Shrub', value='Tree', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (annual)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Graminoid (perennial)', value='Graminoid', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Graminoid (annual)', value='Graminoid', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (annual or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (biennial or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (biennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb (annual or biennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (annual or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Graminoid (annual or perennial)', value='Graminoid', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (biennial or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (annual)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub (annual or perennial)', value='Shrub', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Herb', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Herb (annual or perennial)', value='Vine', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Herb (perennial)', value='Vine', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Graminoid (perennial)', value='Graminoid', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (annual or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Herb (annual)', value='Vine', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Shrub', value='Vine', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Shrub, Herb (perennial)', value='Vine', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Shrub, Herb (biennial or perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Tree, Vine, Shrub', value='Shrub', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Tree, Shrub, Herb (perennial)', value='Wildflower', inplace=True)
counties_init['Plant Type'].replace(
    to_replace='Vine, Shrub, Graminoid (perennial)', value='Graminoid', inplace=True)

counties_init.loc[77973, 'Plant Type'] = 'Tree'
counties_init.loc[93777, 'Plant Type'] = 'Tree'

# %%
counties_unique = counties_init.drop_duplicates(['Scientific Name'])
counties_unique.drop('unique_id', 1, inplace=True)
counties_group = counties_init.groupby('Scientific Name')[
    'unique_id'].apply(list).reset_index()

counties_final = pd.merge(counties_unique, counties_group,
                          on='Scientific Name', how='left')
common_miss = counties_final.loc[pd.isna(counties_final["Common Name"]), :]

# %%

finalj = counties_final.to_json(orient='records', force_ascii=False)
repr(finalj)

final_miss = common_miss.to_json(orient='records', force_ascii=False)
repr(final_miss)

# %%

file = open("us_plants.json", "w")
file.write(finalj)
file.close

file = open("us_plants_miss.json", "w")
file.write(final_miss)
file.close



# %%

us_plants = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/final/us_plants_full.json')


wwf_animals = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/final/wwf_animal_full.json')
inat = pd.read_json(
    '/home/muskrat/Documents/eco_data_copy/main_eco_data/final/inat_full.json')

# %%
inat["kingdom"] = inat["kingdom"].astype("category")

inat["phylum"] = inat["phylum"].astype("category")

inat["class"] = inat["class"].astype("category")

inat["family"] = inat["family"].astype("category")

inat["order"] = inat["order"].astype("category")

inat["scientific_name"] = inat["scientific_name"].astype("category")

# %%
inat = inat[['kingdom', 'phylum', 'class', 'order',
             'family', 'scientific_name', 'unique_id', 'rights']]

us_plants.rename(columns={'Plant Type': 'species_type', 'Scientific Name': 'scientific_name',
                 'Common Name': 'common_name', 'Plant Family': 'family', 'Native Status': 'native_status'}, inplace=True)

wwf_animals.rename(columns={'SPECIES_ID': 'species_id', 'CLASS': 'class', 'ORDER_DESC': 'order', 'FAMILY': 'family',
                   'Scientific_Name': 'scientific_name', 'COMMON_NAME': 'common_name', 'ECOREGION_CODE': 'eco_code', 'ECOREGION_NAME': 'eco_name'}, inplace=True)

us_plants = us_plants[['species_type', 'family',
                       'scientific_name', 'common_name', 'native_status', 'unique_id']]

wwf_animals = wwf_animals[['class', 'order', 'family', 'scientific_name',
                           'common_name', 'species_id', 'eco_code', 'eco_name', 'unique_id']]


# %%

wwf_animals.dropna(subset=['unique_id'], inplace=True)

# %%

# remove duplicates in nested list
inat['unique_id'] = inat.unique_id.map(pd.unique)

us_plants['unique_id'] = us_plants.unique_id.map(pd.unique)

wwf_animals['unique_id'] = wwf_animals.unique_id.map(pd.unique)


# %%

# head = inat.head(n=100)

tester = us_plants['scientific_name'].str.split(' ', expand=True)

tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)

tester['scientific_name'] = tester[0].str.cat(tester[1], sep=" ")

tester = tester.drop([0, 1, 2, 3], axis=1)

us_plants = pd.merge(tester, us_plants, how='inner', on=['scientific_name'])

# %%

tester = wwf_animals['scientific_name'].str.split(' ', expand=True)

tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)


tester['scientific_name'] = tester[0].str.cat(tester[1], sep=" ")

tester = tester.drop([0, 1, 2, 3], axis=1)

wwf_animals = pd.merge(tester, wwf_animals, how='inner',
                       on=['scientific_name'])

# %%

tester = inat['scientific_name'].str.split(' ', expand=True)

tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)

tester.drop(tester[tester[4].notna()].index, inplace=True)

tester['scientific_name'] = tester[0].str.cat(tester[1], sep=" ")

tester = tester.drop([0, 1, 2, 3, 4], axis=1)

inat = pd.merge(tester, inat, how='inner', on=['scientific_name'])

# %%

wwf_animals = wwf_animals.drop(['species_id', 'eco_name', 'eco_code'], axis=1)
# %%

inner_plant = pd.merge(inat, us_plants, how='inner', on=['scientific_name'])

inner_plant = inner_plant.drop(['family_y'], axis=1)

inner_plant.rename(columns={'family_x': 'family'}, inplace=True)

# %%

inner_plant['combined'] = inner_plant['unique_id_x'].apply(
    lambda x: x.tolist()) + inner_plant['unique_id_y'].apply(lambda x: x.tolist())


inner_plant['combined'] = inner_plant['combined'].apply(set).apply(list)

testoo = inner_plant[['unique_id_x', 'unique_id_y', 'combined']]

inner_plant = inner_plant.drop(['unique_id_x', 'unique_id_y'], axis=1)

inner_plant.rename(columns={'combined': 'unique_id'}, inplace=True)
# %%


inner_animal = pd.merge(inat, wwf_animals, how='inner', on=['scientific_name'])

inner_animal = inner_animal.drop(['family_y', 'class_y', 'order_y'], axis=1)

inner_animal.rename(columns={'family_x': 'family',
                    'class_x': 'class', 'order_x': 'order'}, inplace=True)

# %%

inner_animal['combined'] = inner_animal['unique_id_x'].apply(
    lambda x: x.tolist()) + inner_animal['unique_id_y'].apply(lambda x: x.tolist())


inner_animal['combined'] = inner_animal['combined'].apply(set).apply(list)

testoo = inner_animal[['unique_id_x', 'unique_id_y', 'combined']]

inner_animal = inner_animal.drop(['unique_id_x', 'unique_id_y'], axis=1)

inner_animal.rename(columns={'combined': 'unique_id'}, inplace=True)

# %%


tester = inat

# df_1_2 = tester.merge(inner_plant, on=["scientific_name"], how="left", indicator=True)

# df_1_not_2 = df_1_2[df_1_2["_merge"] == "left_only"].drop(columns=["_merge"])

# df_1_not_2 = df_1_not_2.drop(['species_id', 'eco_name', 'eco_code'], axis=1)

df = pd.merge(tester, inner_plant, how='outer', on=[
              'scientific_name'], indicator=True)

df1 = df.loc[df._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x',
                                        'family_x', 'scientific_name', 'common_name', 'species_type', 'unique_id_x', 'rights_x']]

df1.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
           'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights'}, inplace=True)

# df2 = pd.merge(df1, inner_plant, how='inner', on=['scientific_name'])

# %%
df3 = pd.merge(df1, inner_animal, how='outer', on=[
               'scientific_name'], indicator=True)

inat_filter = df3.loc[df3._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x',
                                                  'family_x', 'scientific_name', 'common_name_x', 'species_type', 'unique_id_x', 'rights_x']]

inat_filter.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
                   'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights', 'common_name_x': 'common_name'}, inplace=True)

# testee = df4.drop_duplicates(subset=['scientific_name'])

# %%

df4 = pd.merge(us_plants, inner_plant, how='outer',
               on=['scientific_name'], indicator=True)

us_filter = df4.loc[df4._merge == 'left_only', ['kingdom', 'phylum', 'class', 'order',
                                                'family_y', 'scientific_name', 'common_name_x', 'species_type_x', 'unique_id_x', 'rights']]

us_filter.rename(columns={'family_y': 'family', 'unique_id_x': 'unique_id',
                 'species_type_x': 'species_type', 'common_name_x': 'common_name'}, inplace=True)

# df2 = pd.merge(df1, inner_plant, how='inner', on=['scientific_name'])

# %%
df5 = pd.merge(wwf_animals, inner_animal, how='outer',
               on=['scientific_name'], indicator=True)

wwf_filter = df5.loc[df5._merge == 'left_only', ['kingdom', 'phylum', 'class_x',
                                                 'order_x', 'family_x', 'scientific_name', 'common_name_x', 'unique_id_x', 'rights']]

wwf_filter.rename(columns={'family_x': 'family', 'class_x': 'class', 'order_x': 'order',
                  'unique_id_x': 'unique_id', 'common_name_x': 'common_name'}, inplace=True)

# %%

chromista = inat_filter[(inat_filter['kingdom'] == 'Chromista')]

bacteria = inat_filter[(inat_filter['kingdom'] == 'Bacteria')]

protozoa = inat_filter[(inat_filter['kingdom'] == 'Protozoa')]

virus = inat_filter[(inat_filter['kingdom'] == 'Viruses')]

# %%

# inat_filter.drop(inat_filter.loc[inat_filter['kingdom'] == 'Viruses'].index, inplace=True)

# inat_filter.drop(inat_filter.loc[inat_filter['kingdom'] == 'Viruses'].index, inplace=True)

wwf_filter['species_type'] = np.NAN

inner_animal['species_type'] = np.NAN

inner_plant = inner_plant.drop(['native_status'], axis=1)

frames = [inat_filter, inner_animal, inner_plant, us_filter, wwf_filter]

# concatenate dataframes
final = pd.concat(frames)

# reset index
final.reset_index(drop=True, inplace=True)
# %%


common_miss = final.loc[final['common_name'].isna()]


finalj = final.to_json(orient='records', force_ascii=False)
repr(finalj)

final_miss = common_miss.to_json(orient='records', force_ascii=False)
repr(final_miss)

# %%

file = open("species_prep.json", "w")
file.write(finalj)
file.close

file = open("common_miss_prep_.json", "w")
file.write(final_miss)
file.close

# %%

miss = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/common_miss_prep_4.json')

# %%
head = miss.loc[183500:]

head.reset_index(drop=True, inplace=True)
# %%

# REMOVE 1 FROM RANGE#############
for i in range(len(head)):
    time.sleep(3)
    species = head.loc[i, 'scientific_name']
    response = get_taxa(q=species, rank_level=10)
    time.sleep(3)
    results = response['results']
    if len(results) < 2:
        key_list = [value for elem in results for value in elem.keys()]

        if 'preferred_common_name' in key_list:
            name = [sub['preferred_common_name'] for sub in results]
            head.loc[i, 'common_name'] = name

            print('yes')
            print(i)
        else:
            print('no')
            print(i)

    else:
        key_list = results[0].keys()

        if 'preferred_common_name' in key_list:
            name = results[0]['preferred_common_name']
            head.loc[i, 'common_name'] = name

            print('yes')
            print(i)
        else:
            print('no')
            print(i)

    # if eco_county[i]['eco_counties']:
    #     full_county.append(eco_county[i])
    # print(type(species))
    # print(species)

# response = get_taxa(q='Quercus alba', rank_level=10)
# results = response['results']

# result = results[0]

# key_list = [value for elem in result
#                       for value in elem.keys()]

# key_list = results[0].keys()

# name = results[0]['preferred_common_name']

# %%


final_miss = head.to_json(orient='records', force_ascii=False)


# %%

file = open("head_miss_final_183500_end.json", "w")
file.write(final_miss)
file.close


# %%

ecomap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/wwf_terr_map_data/terr_map.geojson')

# %%
marinemap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/marine_map_data/marine_map.geojson')
# marinemap = gpd.read_file(
#     '/media/muskrat/060A9B8E0A9B78FF/map_final/eco_map.geojson')

# %%


ranger = [*range(826, 826+232)]
ranger2 = [*range(1058, 1058+37)]
ranger3 = ranger2 + ranger
rover = [str(x) for x in ranger3]

marinemap.insert(0, 'unique_id', rover)

# %%

marinemap.to_file("marine_map.geojson", driver='GeoJSON')

# %%


ecomap['name'] = ecomap['ECO_NAME']

ecomap["unique_id"] = ecomap["unique_id"].astype("float32")

ecomap.drop(ecomap[ecomap['unique_id'].isna()].index, inplace=True)

ecomap["unique_id"] = ecomap["unique_id"].astype("int32")

ecomap['unique_id'] = ecomap['unique_id'].apply(str)

ecomap.drop_duplicates(subset=['unique_id'], keep='first', inplace=True)

marinemap['name'] = ''

marinemap.loc[marinemap['TYPE'] == 'PPOW', 'name'] = marinemap['PROVINC']

marinemap.loc[marinemap['TYPE'] == 'MEOW', 'name'] = marinemap['ECOREGION']

econames = pd.concat([ecomap[['unique_id', 'name']], marinemap[[
                     'unique_id', 'name']]], axis=0, keys=['unique_id', 'name'])


# econames.drop_duplicates(subset=['unique_id'], keep='first', inplace=True)

econames.reset_index(drop=True, inplace=True)

# %%
ecokeys = econames.to_json(orient='records', force_ascii=False)
repr(ecokeys)

# %%

file = open("ecoregion_keys.json", "w")
file.write(ecokeys)
file.close

# %%
marinemap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/marine_map_data/marine_map.geojson')

# %%

joined = df.sjoin(marinemap, predicate='within')
# %%
joined = joined.drop(['index_right', 'geometry'], axis=1)

# %%
ecos3 = joined.groupby('scientific_name')[
    'unique_id'].apply(list).reset_index()

ecos4 = joined.groupby('scientific_name')['rights'].apply(list).reset_index()

# ecos5 = joined.groupby(['scientific_name'], as_index=False)
# %%
# ecos5 = ecos5.apply(lambda x: x)
ecos7 = joined.drop_duplicates(subset=['scientific_name'])
ecos7 = ecos7.drop(['unique_id', 'rights'], axis=1)
# %%

ecos6 = pd.merge(ecos3, ecos4, on='scientific_name', how='left').reindex(columns=[
    'scientific_name', 'unique_id', 'rights'])


final = pd.merge(ecos6, ecos7, on='scientific_name', how='left')

final1 = final[final['unique_id'].map(lambda d: len(d)) > 0]

final2 = final1[['kingdom', 'phylum', 'class', 'family',
                 'order', 'scientific_name', 'unique_id', 'rights']]

# %%
finalj = final2.to_json(orient='records', force_ascii=False)
repr(finalj)

# %%

file = open("inat_full_marine.json", "w")
file.write(finalj)
file.close
# %%

inat = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/inat_filtered/inat_full_marine.json')

# %%

inat = inat[['kingdom', 'phylum', 'class', 'order',
             'family', 'scientific_name', 'unique_id', 'rights']]

# %%

inat['unique_id'] = inat.unique_id.map(pd.unique)

# %%

tester = inat['scientific_name'].str.split(' ', expand=True)

tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)

# tester.drop(tester[tester[4].notna()].index, inplace=True)

tester['scientific_name'] = tester[0].str.cat(tester[1], sep=" ")

tester = tester.drop([0, 1, 2, 3], axis=1)

inat = pd.merge(tester, inat, how='inner', on=['scientific_name'])

tester = inat['scientific_name'].str.split(' ', expand=True)

inat['genus'] = tester[0]

# %%

inat.drop(inat.loc[inat['phylum'] == 'Tracheophyta'].index, inplace=True)

inat.drop(inat.loc[inat['phylum'] == 'Basidiomycota'].index, inplace=True)

inat.drop(inat.loc[inat['phylum'] == 'Ascomycota'].index, inplace=True)

inat.drop(inat.loc[inat['phylum'] == 'Bryophyta'].index, inplace=True)

inat.drop(inat.loc[inat['phylum'] == 'Marchantiophyta'].index, inplace=True)

inat.drop(inat.loc[inat['phylum'] == 'Oomycota'].index, inplace=True)

inat.drop(inat.loc[inat['class'] == 'Amphibia'].index, inplace=True)

inat.drop(inat.loc[inat['class'] == 'Insecta'].index, inplace=True)

inat.drop(inat.loc[inat['class'] == 'Diplopoda'].index, inplace=True)

inat.drop(inat.loc[inat['class'] == 'Chilopoda'].index, inplace=True)

inat.drop(inat.loc[inat['class'] == 'Collembola'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Lepidoptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Passeriformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Coleoptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Hymenoptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Hemiptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Araneae'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Diptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Odonata'].index, inplace=True)
# %%

inat.drop(inat.loc[inat['order'] == 'Orthoptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Stylommatophora'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Rodentia'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Accipitriformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Columbiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Piciformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Apodiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Psittaciformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Coraciiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Cuculiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Chiroptera'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Strigiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Galliformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Scorpiones'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Falconiformes'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Opiliones'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Primates'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Caprimulgiformes'].index, inplace=True)
# %%

inat.drop(inat.loc[inat['order'] == 'Diprotodontia'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Soricomorpha'].index, inplace=True)

inat.drop(inat.loc[inat['order'] == 'Lagomorpha'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Colubridae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Scincidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Gekkonidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Lacertidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Dactyloidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Teiidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Agamidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Viperidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Bovidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Phyllodactylidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Tropiduridae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Varanidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Canidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Liolaemidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Chamaeleonidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Porcellionidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Eriophyidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Sphaerodactylidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Ixodidae'].index, inplace=True)

inat.drop(inat.loc[inat['family'] == 'Boidae'].index, inplace=True)


# %%
species = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_6.json')

# tester = pd.read_json('/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/tester2.json')

# %%

inner_plant = pd.merge(species, inat, how='inner', on=['scientific_name'])

inner_plant = inner_plant.drop(
    ['family_y', 'phylum_y', 'class_y', 'order_y', 'rights_y', 'genus_y', 'kingdom_y'], axis=1)

inner_plant.rename(columns={'family_x': 'family', 'phylum_x': 'phylum', 'class_x': 'class',
                   'rights_x': 'rights', 'kingdom_x': 'kingdom', 'genus_x': 'genus', 'order_x': 'order'}, inplace=True)


# %%

inner_plant['combined'] = inner_plant['unique_id_x'] + \
    inner_plant['unique_id_y'].apply(lambda x: x.tolist())

inner_plant['combined'] = inner_plant['combined'].apply(set).apply(list)

testoo = inner_plant[['unique_id_x', 'unique_id_y', 'combined']]

inner_plant = inner_plant.drop(['unique_id_x', 'unique_id_y'], axis=1)

inner_plant.rename(columns={'combined': 'unique_id'}, inplace=True)

# %%

tester = inat

# df_1_2 = tester.merge(inner_plant, on=["scientific_name"], how="left", indicator=True)

# df_1_not_2 = df_1_2[df_1_2["_merge"] == "left_only"].drop(columns=["_merge"])

# df_1_not_2 = df_1_not_2.drop(['species_id', 'eco_name', 'eco_code'], axis=1)

df = pd.merge(tester, inner_plant, how='outer', on=[
              'scientific_name'], indicator=True)

df1 = df.loc[df._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x', 'genus_x',
                                        'family_x', 'scientific_name', 'common_name', 'species_type', 'unique_id_x', 'rights_x']]

df1.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
           'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights', 'genus_x': 'genus'}, inplace=True)

# df2 = pd.merge(df1, inner_plant, how='inner', on=['scientific_name'])

# %%
df3 = pd.merge(species, inner_plant, how='outer', on=[
               'scientific_name'], indicator=True)

inat_filter = df3.loc[df3._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x',
                                                  'family_x', 'scientific_name', 'common_name_x', 'species_type_x', 'unique_id_x', 'rights_x', 'genus_x']]

inat_filter.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
                   'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights', 'genus_x': 'genus', 'common_name_x': 'common_name', 'species_type_x': 'species_type'}, inplace=True)

# testee = df4.drop_duplicates(subset=['scientific_name'])


# %%

frames = [inat_filter, inner_plant, df1]

# concatenate dataframes
final = pd.concat(frames)

# reset index
final.reset_index(drop=True, inplace=True)

# %%

common_miss = final.loc[final['common_name'].isna()]


finalj = final.to_json(orient='records', force_ascii=False)
# repr(finalj)

final_miss = common_miss.to_json(orient='records', force_ascii=False)
# repr(final_miss)

# %%

file = open("species_prep_8.json", "w")
file.write(finalj)
file.close

file = open("common_miss_prep_3.json", "w")
file.write(final_miss)
file.close

# %%

# dp = dd.read_parquet('/media/muskrat/060A9B8E0A9B78FF/obis/obis_park/obis_20211022.parquet')
df = vaex.open(
    '/media/muskrat/060A9B8E0A9B78FF/obis/obis_park/obis_20211022.parquet')

df
df.describe()
df.head()
df.tail()
df.column_names
df.drop(['AphiaID', 'institutionCode'], inplace=True)

df.drop(['minimumDepthInMeters', 'maximumDepthInMeters', 'shoredistance', 'bathymetry', 'sst', 'sss', 'marine', 'brackish', 'freshwater', 'terrestrial', 'taxonRank', 'redlist_category', 'superdomain', 'domain', 'subspecies',
         'natio',
         'variety',
         'subvariety',
         'forma',
         'subforma', 'institutionID',
         'collectionID',
         'datasetID',
         'collectionCode',
         'datasetName',
         'ownerInstitutionCode', 'informationWithheld',
         'dataGeneralizations',
         'dynamicProperties',
         'materialSampleID',
         'occurrenceID',
         'catalogNumber',
         'occurrenceRemarks',
         'recordNumber',
         'recordedBy',
         'recordedByID',
         'individualCount',
         'organismQuantity',
         'organismQuantityType',
         'sex',
         'lifeStage',
         'reproductiveCondition',
         'behavior', 'occurrenceStatus',
         'preparations',
         'disposition',
         'otherCatalogNumbers',
         'associatedMedia',
         'associatedReferences',
         'associatedSequences',
         'associatedTaxa',
         'organismID',
         'organismName',
         'organismScope',
         'associatedOccurrences',
         'associatedOrganisms',
         'previousIdentifications',
         'organismRemarks',
         'eventID',
         'parentEventID',
         'samplingProtocol',
         'sampleSizeValue',
         'sampleSizeUnit', 'verbatimEventDate',
         'habitat',
         'fieldNumber',
         'fieldNotes',
         'eventRemarks',
         'locationID',
         'higherGeographyID',
         'higherGeography',
         'continent',
         'waterBody',
         'islandGroup',
         'island',
         'country',
         'countryCode',
         'stateProvince',
         'county',
         'municipality',
         'locality',
         'verbatimLocality',
         'verbatimElevation',
         'minimumElevationInMeters',
         'maximumElevationInMeters',
         'verbatimDepth',
         'minimumDistanceAboveSurfaceInMeters',
         'maximumDistanceAboveSurfaceInMeters',
         'locationAccordingTo',
         'locationRemarks', 'geologicalContextID',
         'earliestEonOrLowestEonothem',
         'latestEonOrHighestEonothem',
         'earliestEraOrLowestErathem',
         'latestEraOrHighestErathem',
         'earliestPeriodOrLowestSystem',
         'latestPeriodOrHighestSystem',
         'earliestEpochOrLowestSeries',
         'latestEpochOrHighestSeries',
         'earliestAgeOrLowestStage',
         'latestAgeOrHighestStage',
         'lowestBiostratigraphicZone',
         'highestBiostratigraphicZone',
         'lithostratigraphicTerms',
         'group',
         'formation',
         'member',
         'bed',
         'identificationID',
         'identifiedBy',
         'identifiedByID',
         'dateIdentified',
         'identificationReferences',
         'identificationRemarks',
         'identificationQualifier',
         'identificationVerificationStatus',
         'typeStatus',
         'taxonID',
         'scientificNameID',
         'acceptedNameUsageID',
         'parentNameUsageID',
         'originalNameUsageID',
         'nameAccordingToID',
         'namePublishedInID',
         'taxonConceptID',
         'acceptedNameUsage',
         'parentNameUsage',
         'originalNameUsage',
         'nameAccordingTo',
         'namePublishedIn',
         'namePublishedInYear',
         'higherClassification',
         'specificEpithet',
         'infraspecificEpithet',
         'verbatimTaxonRank',
         'scientificNameAuthorship',
         'nomenclaturalCode',
         'taxonomicStatus',
         'nomenclaturalStatus', ], inplace=True)

df.drop(['dataset_id', 'subkingdom',
         'infrakingdom', 'phylum_division',
         'subphylum_subdivision',
         'subphylum',
         'infraphylum',
         'parvphylum',
         'gigaclass',
         'megaclass',
         'superclass', 'subclass',
         'infraclass',
         'subterclass',
         'superorder', 'suborder',
         'infraorder',
         'parvorder',
         'superfamily', 'subfamily',
         'supertribe',
         'tribe',
         'subtribe', 'subgenus',
         'section',
         'subsection',
         'series', 'samplingEffort', 'pointRadiusSpatialFit', 'footprintSpatialFit', ], inplace=True)

df.drop(['date_start',
         'date_mid',
         'date_end', 'originalScientificName', 'language', 'eventDate',
         'eventTime',
         'startDayOfYear',
         'endDayOfYear', 'month',
         'day', ], inplace=True)

# %%

df.export_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_init/obis.parquet')

# %%

df = vaex.open(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_init/obis.parquet')

# %%

df.drop(['verbatimCoordinates',
         'verbatimLatitude',
         'verbatimLongitude',
         'verbatimCoordinateSystem',
         'verbatimSRS', 'georeferenceSources',
         'georeferenceVerificationStatus', ], inplace=True)

# %%

df_year = df[df.date_year > 2000]
df_year
df_year.describe()

# %%

df_year.drop(['establishmentMeans'], inplace=True)
df_year.column_names

df_year.drop(['y', 'year'], inplace=True)

# %%

df_year.export_csv(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_csv_year/obis_year.csv')
# %%

df_year.export_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_year/obis_year.parquet')

# %%

vee = vaex.open(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_year/obis_year.parquet')

# %%

vee.describe()

vee.dropped.value_counts()

vee.absence.value_counts()

vee.drop(['dropped', 'absence'], inplace=True)

vee['type'].value_counts()

vee.drop(['type'], inplace=True)

vee.modified.value_counts()

vee.drop(['modified'], inplace=True)

vee.column_names

vee.geodeticDatum.value_counts()

vee.coordinatePrecision.value_counts()

vee.footprintWKT.value_counts()

vee.footprintSRS.value_counts()

vee.georeferencedBy.value_counts()

vee.georeferencedDate.value_counts()

vee.georeferenceProtocol.value_counts()

vee.georeferenceRemarks.value_counts()

vee.taxonRemarks.value_counts()

vee.references.value_counts()

vee.license.value_counts()

vee.accessRights.value_counts()

vee.bibliographicCitation.value_counts()

vee.basisOfRecord.value_counts()

vee.flags.value_counts()

vee.scientificName.value_counts()

vee.drop(['footprintWKT', 'footprintSRS', 'georeferencedBy', 'georeferencedDate',
         'georeferenceProtocol', 'georeferenceRemarks', 'taxonRemarks', 'references'], inplace=True)

# %%

vee['coord'] = vee.coordinateUncertaintyInMeters.astype('float32')

vee_meter = vee[vee.coord < 390]

vee_meter.describe()

vee_meter.scientificName.value_counts()

vee_meter.drop(['coord'], inplace=True)

# %%

vee_meter.export_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_geo/obis_geo.parquet')

# %%

obis = pd.read_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_geo/obis_geo.parquet')

# %%
obis.dropna(subset=['species'], inplace=True)

science = obis[['scientificName']]
species = obis[['species']]

science.rename(columns={'scientificName': 'species'}, inplace=True)

obis.reset_index(drop=True, inplace=True)
# %%

obis.drop(obis.loc[obis['geodeticDatum'] == 'NAD83'].index, inplace=True)

obis.drop(obis.loc[obis['geodeticDatum'] == 'unknown'].index, inplace=True)

obis.drop(obis.loc[obis['geodeticDatum'] ==
          'not recorded'].index, inplace=True)

obis.drop(obis.loc[obis['geodeticDatum'] == 'AGD 66'].index, inplace=True)

# %%
obis.drop(obis.loc[obis['license'] ==
          'Copyright Glacier Bay National Park Humpback Whale Monitoring Program. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Russian Cetacean Habitat Project. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Whales of Guerrero (contact: Katherina Audley). No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Ma. Eugenia Rodríguez Vázquez. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Nico Ransome. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright IWC Southern Ocean Whale and Ecosystem Research (SOWER) cruises. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright IWC Pacific Ocean Whale and Ecosystem Research (POWER) cruises. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Joelle De Weerdt. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Tasli Shaw. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Amy Kennedy. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright David Paton. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Amy Engelhaupt. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright HDR . No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Leanne Maffesoni. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Mithriel MacKay. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Ran Dembo. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright OMMAG. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Krista Rossow. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Olaf Meynecke. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Daniel Burns. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright ralph pace. No use without permission.'].index, inplace=True)

obis.drop(obis.loc[obis['license'] ==
          'Copyright Tory Kallman. No use without permission.'].index, inplace=True)

obis.reset_index(drop=True, inplace=True)

# %%
coord = obis[['scientificName']].value_counts()

coord2 = obis[['species']].value_counts()

coord = pd.DataFrame(coord)

coord.reset_index(inplace=True)

coord2 = pd.DataFrame(coord2)

coord2.reset_index(inplace=True)

coord.rename(columns={'scientificName': 'species'}, inplace=True)

inner = pd.merge(coord, coord2, how="outer", on='species', indicator=True)

obis['scientific_name'] = obis['scientificName']

obis['scientific_name'].fillna(obis['species'], inplace=True)

# obis['scientific_name'].isna().sum()

# %%
obis = obis.drop(['coordinateUncertaintyInMeters', 'coordinatePrecision', 'date_year',
                 'id', 'basisOfRecord', 'geodeticDatum', 'flags', 'scientificName', 'species'], axis=1)

# %%

obis['scientific_name'] = obis['scientific_name'].str.replace(r" \(.*\)", "")

tester = obis

tester[[0, 1, 2, 3, 4, 5]] = tester['scientific_name'].str.split(
    ' ', expand=True)


tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)

tester.drop(tester[tester[4].notna()].index, inplace=True)

tester.drop(tester[tester[5].notna()].index, inplace=True)

tester['scientific_name'] = tester[0].str.cat(tester[1], sep=" ")

tester['genus'] = tester[0]

tester = tester.drop([0, 1, 2, 3, 4, 5], axis=1)

# %%

tester.rename(columns={'vernacularName': 'common_name'}, inplace=True)

obis = tester.drop(['accessRights'], axis=1)

# %%
obis['rightsHolder'].fillna(obis['bibliographicCitation'], inplace=True)
rights = ['license', 'rightsHolder']
obis['rights'] = obis[rights].to_dict(orient='records')
obis = obis.drop(['license', 'rightsHolder', 'bibliographicCitation'], axis=1)

# %%

obis.to_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/obis_park_filtered/obis_filtered.parquet')

# %%
pp = pd.read_parquet(
    '/media/muskrat/060A9B8E0A9B78FF/obis/obis_park_filtered/obis_filtered.parquet')

# %%


gdf = gpd.GeoDataFrame(pp, geometry=gpd.points_from_xy(
    pp.decimalLongitude, pp.decimalLatitude))

gdf = gdf.drop(['decimalLatitude', 'decimalLongitude'], axis=1)

df = gdf

df = gpd.GeoDataFrame(df, crs='epsg:4326')

df.drop(df[df['geometry'].is_empty].index, inplace=True)

# %%
marinemap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/marine_map_data/marine_map.geojson')

# %%

joined = df.sjoin(marinemap, predicate='within')
# %%
joined = joined.drop(['index_right', 'geometry'], axis=1)

# %%
ecos3 = joined.groupby('scientific_name')[
    'unique_id'].apply(list).reset_index()

ecos4 = joined.groupby('scientific_name')['rights'].apply(list).reset_index()

# ecos5 = joined.groupby(['scientific_name'], as_index=False)
# %%
# ecos5 = ecos5.apply(lambda x: x)
ecos7 = joined.drop_duplicates(subset=['scientific_name'])
ecos7 = ecos7.drop(['unique_id', 'rights'], axis=1)
# %%

ecos6 = pd.merge(ecos3, ecos4, on='scientific_name', how='left').reindex(columns=[
    'scientific_name', 'unique_id', 'rights'])


final = pd.merge(ecos6, ecos7, on='scientific_name', how='left')

final1 = final[final['unique_id'].map(lambda d: len(d)) > 0]

final2 = final1[['kingdom', 'phylum', 'class', 'order', 'family',
                 'genus', 'scientific_name', 'common_name', 'unique_id', 'rights']]

# %%
finalj = final2.to_json(orient='records', force_ascii=False)

# %%

file = open("obis_full.json", "w")
file.write(finalj)
file.close

# %%

species = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_8.json')

obis = pd.read_json('/media/muskrat/060A9B8E0A9B78FF/obis/obis_full.json')

# %%

obis["kingdom"] = obis["kingdom"].astype("category")

obis["phylum"] = obis["phylum"].astype("category")

obis["class"] = obis["class"].astype("category")

obis["family"] = obis["family"].astype("category")

obis["order"] = obis["order"].astype("category")

obis["genus"] = obis["genus"].astype("category")

obis["scientific_name"] = obis["scientific_name"].astype("category")

# %%

species["kingdom"] = species["kingdom"].astype("category")

species["phylum"] = species["phylum"].astype("category")

species["class"] = species["class"].astype("category")

species["family"] = species["family"].astype("category")

species["order"] = species["order"].astype("category")

species["genus"] = species["genus"].astype("category")

species["scientific_name"] = species["scientific_name"].astype("category")

# %%
obis['unique_id'] = obis.unique_id.map(pd.unique)


# %%

inner_plant = pd.merge(species, obis, how='inner', on=['scientific_name'])

inner_plant = inner_plant.drop(
    ['family_y', 'phylum_y', 'class_y', 'order_y', 'genus_y', 'kingdom_y'], axis=1)

inner_plant.rename(columns={'family_x': 'family', 'phylum_x': 'phylum', 'class_x': 'class',
                   'kingdom_x': 'kingdom', 'genus_x': 'genus', 'order_x': 'order'}, inplace=True)


# %%

inner_plant['combined'] = inner_plant['unique_id_x'] + \
    inner_plant['unique_id_y'].apply(lambda x: x.tolist())

inner_plant['combined'] = inner_plant['combined'].apply(set).apply(list)

testoo = inner_plant[['unique_id_x', 'unique_id_y', 'combined']]

inner_plant = inner_plant.drop(['unique_id_x', 'unique_id_y'], axis=1)

inner_plant.rename(columns={'combined': 'unique_id'}, inplace=True)

inner_plant['name'] = inner_plant['common_name_x']

inner_plant['name'].fillna(inner_plant['common_name_y'], inplace=True)

inner_plant = inner_plant.drop(['common_name_x', 'common_name_y'], axis=1)

inner_plant.rename(columns={'name': 'common_name'}, inplace=True)

inner_plant['combined'] = inner_plant['rights_x'] + inner_plant['rights_y']

inner_plant = inner_plant.drop(['rights_x', 'rights_y'], axis=1)

inner_plant.rename(columns={'combined': 'rights'}, inplace=True)

# %%

tester = obis

# df_1_2 = tester.merge(inner_plant, on=["scientific_name"], how="left", indicator=True)

# df_1_not_2 = df_1_2[df_1_2["_merge"] == "left_only"].drop(columns=["_merge"])

# df_1_not_2 = df_1_not_2.drop(['species_id', 'eco_name', 'eco_code'], axis=1)

df = pd.merge(tester, inner_plant, how='outer', on=[
              'scientific_name'], indicator=True)

df1 = df.loc[df._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x', 'genus_x',
                                        'family_x', 'scientific_name', 'common_name_x', 'species_type', 'unique_id_x', 'rights_x']]

df1.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
           'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights', 'genus_x': 'genus', 'common_name_x': 'common_name'}, inplace=True)

# df2 = pd.merge(df1, inner_plant, how='inner', on=['scientific_name'])

# %%
df3 = pd.merge(species, inner_plant, how='outer', on=[
               'scientific_name'], indicator=True)

inat_filter = df3.loc[df3._merge == 'left_only', ['kingdom_x', 'phylum_x', 'class_x', 'order_x',
                                                  'family_x', 'scientific_name', 'common_name_x', 'species_type_x', 'unique_id_x', 'rights_x', 'genus_x']]

inat_filter.rename(columns={'kingdom_x': 'kingdom', 'phylum_x': 'phylum', 'family_x': 'family', 'class_x': 'class',
                   'order_x': 'order', 'unique_id_x': 'unique_id', 'rights_x': 'rights', 'genus_x': 'genus', 'common_name_x': 'common_name', 'species_type_x': 'species_type'}, inplace=True)

# testee = df4.drop_duplicates(subset=['scientific_name'])


# %%

frames = [inat_filter, inner_plant, df1]

# concatenate dataframes
final = pd.concat(frames)

# reset index
final.reset_index(drop=True, inplace=True)

# %%

df1 = pd.DataFrame(final[199000:])

# %%

common_miss = final.loc[final['common_name'].isna()]

common_miss = common_miss.drop(['rights', 'unique_id'], axis=1)

# %%

finalj = df1.to_json(orient='records', force_ascii=False)

# %%

file = open("species_prep_22.json", "w")
file.write(finalj)
file.close
# %%

final_miss = common_miss.to_json(orient='records', force_ascii=False)
# repr(final_miss)

# %%


file = open("common_miss_prep_4.json", "w")
file.write(final_miss)
file.close
# %%

df1 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_init/species_prep_9.json')

# %%

df = df1['rights'].to_list()

# remove duplicates from list of counties and states
for j in range(len(df)):

    df[j] = [i for n, i in enumerate(
        df[j]) if i not in df[j][n + 1:]]

# %%

df2 = df
for j in range(len(df2)):
    x = df2[j]
    # print(x)
    for i in range(len(x)):
        d = x[i]
        # print(d)
        for key, value in dict(d).items():
            if value is None:
                del d[key]

df3 = df2

df4 = []
for j in range(len(df3)):
    d = df3[j]
    goodTweets = []
    for tweet in d:
        if tweet:
            goodTweets.append(tweet)

    df4.append(goodTweets)
    # for i in range(len(d)):
    #     if d[i]:
    #         print(d[i])
    #     else:
    #         del d[i].index(i)


eco_counties_df = pd.Series(df4, name='rights_final')

df1['rights'] = eco_counties_df

# %%

finalj = df1.to_json(orient='records', force_ascii=False)

# %%

file = open("species_prep_final_14.json", "w")
file.write(finalj)
file.close

# %%
df1 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_1.json')

# %%
df2 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_2.json')

# %%
df3 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_3.json')

# %%
df4 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_4.json')

# %%
df5 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_5.json')

# %%
df6 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_6.json')

# %%
df7 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_7.json')

# %%
df8 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_8.json')

# %%
df9 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_9.json')

# %%
df10 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_10.json')

# %%
df11 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_11.json')

# %%
df12 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_12.json')

# %%
df13 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_13.json')

# %%
df14 = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final_14.json')

# %%

frames = [df1, df2, df3, df4, df5, df6, df7,
          df8, df9, df10, df11, df12, df13, df14]

# concatenate dataframes
final = pd.concat(frames)

# reset index
final.reset_index(drop=True, inplace=True)
# %%

finalj = final.to_json(orient='records', force_ascii=False)

# %%

file = open("species_prep_final.json", "w")
file.write(finalj)
file.close
# %%
ecomap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/maptest/terr_map.geojson')

# %%
marinemap = gpd.read_file(
    '/media/muskrat/060A9B8E0A9B78FF/maptest/marine_map.geojson')

# %%
ecomap['name'] = ecomap['ECO_NAME']

ecomap["unique_id"] = ecomap["unique_id"].astype("float32")

ecomap['unique_id'] = pd.to_numeric(ecomap['unique_id'], downcast='integer')

ecomap["unique_id"] = ecomap["unique_id"].convert_dtypes()

ecomap['unique_id'] = ecomap['unique_id'].astype(str)

ecomap.drop_duplicates(subset=['unique_id'], keep='first', inplace=True)

marinemap['name'] = ''

marinemap.loc[marinemap['TYPE'] == 'PPOW', 'name'] = marinemap['PROVINC']

marinemap.loc[marinemap['TYPE'] == 'MEOW', 'name'] = marinemap['ECOREGION']

marinemap = marinemap[['unique_id', 'name', 'TYPE', 'geometry']]

ecomap['TYPE'] = 'TEOW'

ecomap = ecomap[['unique_id', 'name', 'TYPE', 'geometry']]


# %%

marinemap.to_file("marine_map.geojson", driver='GeoJSON')

ecomap.to_file("terr_map.geojson", driver='GeoJSON')

# %%
df = pd.read_json(
    '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_prep_final/species_prep_final.json')

# %%

path_to_json = '/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/common_names' 

json_pattern = os.path.join(path_to_json,'*.json')
file_list = glob.glob(json_pattern)

# dfs = [] # an empty list to store the data frames
# for file in file_list:
#     data = pd.read_json(file, lines=True) # read data frame from json file
#     dfs.append(data) # append the data frame to the list

# temp = pd.concat(dfs, ignore_index=True) # concatenate all the data frames in the list.

dfs = []
for file in file_list:
    with open(file) as f:
        json_data = pd.json_normalize(json.loads(f.read()))
    dfs.append(json_data)
names = pd.concat(dfs, ignore_index=True)


# %%

names.drop('6', axis=1, inplace=True)

# %%

names = names[['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'scientific_name', 'common_name', 'species_type']]

df = df[['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'scientific_name', 'common_name', 'species_type', 'unique_id', 'rights']]

#%%

miss = names[['scientific_name', 'common_name']]

scientific = df[['scientific_name', 'common_name']]

#%%

test = scientific.merge(miss, how='outer', on='scientific_name', indicator=True)

miss_extra = test[['common_name_x']].notna()
miss_extra = scientific.loc[scientific['common_name'].notna()]

test.common_name_x.fillna(test.common_name_y, inplace=True)

duplicateRows = test[test.duplicated(['scientific_name'])]

extra = test.loc[test['_merge'] == 'left_only']

test2 = miss.merge(scientific, how='left', on='scientific_name', indicator=True)

test2.drop('common_name_y', axis=1, inplace=True)

test2.drop('_merge', axis=1, inplace=True)

test2.rename(columns={'common_name_x': 'common_name'}, inplace=True)

extra.drop('common_name_y', axis=1, inplace=True)

extra.drop('_merge', axis=1, inplace=True)

extra.rename(columns={'common_name_x': 'common_name'}, inplace=True)

test3 = test2.merge(extra, how='outer', on='scientific_name', indicator=True)

duplicateRows1 = test3[test3.duplicated(['scientific_name'])]

duplicateRows2 = extra[extra.duplicated(['scientific_name'])]

test3.drop_duplicates(subset=['scientific_name'], keep='last', inplace=True)

test3.common_name_x.fillna(test3.common_name_y, inplace=True)

test3.drop('common_name_y', axis=1, inplace=True)

test3.drop('_merge', axis=1, inplace=True)

test3.rename(columns={'common_name_x': 'common_name'}, inplace=True)

test4 = test3.merge(df, how='outer', on='scientific_name', indicator=True)

test4.drop('common_name_y', axis=1, inplace=True)

test4.drop('_merge', axis=1, inplace=True)

test4.rename(columns={'common_name_x': 'common_name'}, inplace=True)

final = test4[['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'scientific_name', 'common_name', 'species_type', 'unique_id', 'rights']]

final['same'] = final['scientific_name'] == final['common_name']

same = final.loc[final['same'] == True]
same = same.index

final.loc[same, 'common_name'] = 'nan'



#%%
final.reset_index(drop=True, inplace=True)

 #%%

finalj = final.to_json(orient='records', force_ascii=False)

# %%

file = open("species_v1.json", "w")
file.write(finalj)
file.close





