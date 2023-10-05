# 400 meter coordinate accuracy
# 1923 earliest year
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

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

df_path = input("Enter the file path: ")
df = pd.read_parquet(df_path)

common_miss = df.loc[df["common_name"].isna()]


head = common_miss.loc[183500:]

head.reset_index(drop=True, inplace=True)
# %%

# REMOVE 1 FROM RANGE#############
for i in range(len(head)):
    time.sleep(3)
    species = head.loc[i, "scientific_name"]
    response = get_taxa(q=species, rank_level=10)
    time.sleep(3)
    results = response["results"]
    if len(results) < 2:
        key_list = [value for elem in results for value in elem.keys()]

        if "preferred_common_name" in key_list:
            name = [sub["preferred_common_name"] for sub in results]
            head.loc[i, "common_name"] = name

            print("yes")
            print(i)
        else:
            print("no")
            print(i)

    else:
        key_list = results[0].keys()

        if "preferred_common_name" in key_list:
            name = results[0]["preferred_common_name"]
            head.loc[i, "common_name"] = name

            print("yes")
            print(i)
        else:
            print("no")
            print(i)


# response = get_taxa(q='Quercus alba', rank_level=10)
# results = response['results']

# result = results[0]

# key_list = [value for elem in result
#                       for value in elem.keys()]

# key_list = results[0].keys()

# name = results[0]['preferred_common_name']

# %%


df_miss = head.to_json(orient="records", force_ascii=False)


# %%

file = open("head_miss_df_183500_end.json", "w")
file.write(df_miss)
file.close
