# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# 400 meter coordinate accuracy
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






# file = open("eco.json", "w")
# file.write(eco)
# file.close



# %%





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





