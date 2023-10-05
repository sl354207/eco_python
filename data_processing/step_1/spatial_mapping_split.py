# 400 meter coordinate accuracy
# 1923 earliest year
import pandas as pd
import geopandas as gpd
from dask import dataframe as dd
import dask_geopandas

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

# %%
# for loop that runs from 5000000 to 22000000 in 1,000,000 increments
for i in range(50000000, 220000000, 10000000):
    # read in cleaned parquet
    # df_path = string with variable i in it

    # UPDATE FILE PATH
    df_path = f"/PATH/split_{i}.parquet"

    df = dd.read_parquet(df_path)

    map_path = "/PATH/ecomap_final/eco_map.geojson"

    map = gpd.read_file(map_path)

    map.crs

    # convert to dask_geopandas df
    df_gpd = dask_geopandas.from_dask_dataframe(df)

    del df, df_path

    df_gpd = df_gpd.set_geometry(
        dask_geopandas.points_from_xy(df_gpd, "decimalLongitude", "decimalLatitude")
    )

    df_gpd = df_gpd.drop(["decimalLatitude", "decimalLongitude"], axis=1)

    df_gpd = df_gpd.set_crs("EPSG:4326")

    df_gpd.crs

    df_gpd = df_gpd.compute()

    df_gpd = df_gpd.sjoin(map, predicate="within")

    del map, map_path

    df_gpd = df_gpd.drop(["index_right", "geometry", "name", "TYPE"], axis=1)

    rights = ["license", "rightsHolder"]
    df_gpd["rights"] = df_gpd[rights].to_dict(orient="records")
    df_gpd = df_gpd.drop(["license", "rightsHolder"], axis=1)

    group_id = df_gpd.groupby("species")["unique_id"].apply(list).reset_index()

    group_rights = df_gpd.groupby("species")["rights"].apply(list).reset_index()

    unique_species = df_gpd.drop_duplicates(subset=["species"])
    unique_species = unique_species.drop(["unique_id", "rights"], axis=1)

    del df_gpd, rights

    merged_groups = pd.merge(group_id, group_rights, on="species", how="left").reindex(
        columns=["species", "unique_id", "rights"]
    )

    del group_id, group_rights

    total_merge = pd.merge(merged_groups, unique_species, on="species", how="left")

    del merged_groups, unique_species

    # remove duplicates values from list in unique_id column
    total_merge["unique_id"] = total_merge["unique_id"].apply(set).apply(list)

    rights = total_merge["rights"].to_list()

    for j in range(len(rights)):
        print(j)
        rights[j] = [i for n, i in enumerate(rights[j]) if i not in rights[j][n + 1 :]]

    # convert to series
    rights = pd.Series(rights, name="rights")

    total_merge["rights"] = rights

    del rights, j

    # REMOVE "<NA>" VALUES FROM UNIQUE_ID COLUMN

    # df['unique_id'].apply(lambda x: print(x) if ('<NA>' in x) else x)

    # df["unique_id"] = df["unique_id"].apply(
    #     lambda x: x[x != "<NA>"] if ("<NA>" in x) else x
    # )

    # rename species column to scientific_name
    total_merge = total_merge.rename(columns={"species": "scientific_name"})

    total_merge = total_merge[
        [
            "kingdom",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
            "scientific_name",
            "unique_id",
            "rights",
        ]
    ]

    # read cleaned df into a parquet file where user can input the file path

    # UPDATE FILE PATH
    df_spatial_path = f"/PATH/data_processing/step_1/gbif_occurences/occurences_1.{i + 10000000}.parquet"

    # write df to parquet file using pandas to_parquet
    total_merge.to_parquet(df_spatial_path)

    del total_merge, df_spatial_path
# %%
