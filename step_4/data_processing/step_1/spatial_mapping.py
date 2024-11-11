# %%
# 400 meter coordinate accuracy
# 1924 earliest year
import pandas as pd
import geopandas as gpd
import os

os.environ["PROJ_LIB"] = "/home/muskrat/miniconda3/envs/eco/share/proj"

# from dask import dataframe as dd

# import dask_geopandas

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

# %%
# create a list of all files in the directory
base_path = input("Enter the file base path: ")
files = os.listdir(base_path)

# %%
map_path = input("Enter the map path: ")

# %%
final_path = input("Enter the final file path: ")


# %%

map = gpd.read_file(map_path)

# %%
for file in files:
    print(file)

    df = pd.read_parquet(f"{base_path}/{file}")

    df = gpd.GeoDataFrame(df)

    df["geometry"] = gpd.points_from_xy(df["decimalLongitude"], df["decimalLatitude"])

    df = df.set_geometry("geometry")

    df = df.set_crs("EPSG:4326")

    df = df.drop(["decimalLatitude", "decimalLongitude"], axis=1)
    # print(df.head())

    df = df.sjoin(map, predicate="within")

    # ecoregions
    # remove rows with <NA> in unique_id from df
    # df = df[df["unique_id"] != "<NA>"]

    # freshwater
    # df = df[df["id"] != "<NA>"]

    # soil
    df = df[df["id"] != "<NA>"]

    # ecoregions
    # df = df.drop(["index_right", "geometry", "name", "TYPE"], axis=1)

    # freshwater
    # df = df.drop(["index_right", "geometry", "name"], axis=1)

    # soil
    df = df.drop(
        [
            "index_right",
            "geometry",
            "dominant_soil_type_percentage",
            "soil_texture",
            "soil_slope",
            "soil_id",
            "FAOSOIL",
        ],
        axis=1,
    )

    # divider

    # ecoregions
    # df = df.groupby(
    #     ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
    #     as_index=False,
    #     dropna=False,
    # ).apply(
    #     lambda x: pd.Series(
    #         {
    #             "unique_id": list(x["unique_id"].unique()),
    #             "rights": list(
    #                 x[["unique_id", "rights"]].drop_duplicates(
    #                     "unique_id", keep="first"
    #                 )["rights"]
    #             ),
    #         }
    #     )
    # )

    # freshwater
    # df = df.groupby(
    #     ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
    #     as_index=False,
    #     dropna=False,
    # ).apply(
    #     lambda x: pd.Series(
    #         {
    #             "id": list(x["id"].unique()),
    #         }
    #     )
    # )

    # soil
    df = df.groupby(
        ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
        as_index=False,
        dropna=False,
    ).apply(
        lambda x: pd.Series(
            {
                "id": list(x["id"]),
                "specific_soil_name": list(x["specific_soil_name"]),
                "dominant_soil_name": list(x["dominant_soil_name"]),
            }
        )
    )

    # divider

    # ecoregions
    # df = df.rename(
    #     columns={"species": "scientific_name", "unique_id": "observed_ecoregions"}
    # )

    # freshwater
    # df = df.rename(
    #     columns={"species": "scientific_name", "id": "freshwater_ecoregions"}
    # )

    # soil
    df = df.rename(columns={"species": "scientific_name", "id": "soil_id"})

    # divider

    df.to_parquet(f"{final_path}/{file}")


# %%
