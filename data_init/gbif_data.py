# %%
import pandas as pd
import numpy as np
import geopandas as gpd
from dask import dataframe as dd
import dask_geopandas
from shapely import wkt
import vaex


pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_seq_items', 100)

# %%
# ask user to enter the file path and store the response in a variable
file_input_path = input("Enter the file path: ")


# %%
# read the file using vaex.from_csv and use tab as separator
# df = vaex.from_csv(
#     "/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/0083459-230530130749713.csv",
#     sep="\t",
#     chunk_size=3,
#     # nrows=10,
#     # engine="python",
# )
# df = pd.read_csv(
#     "/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/0083459-230530130749713.csv",
#     sep="\t",
#     nrows=3,
# )
# df = vaex.from_csv(
#     "/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/0083459-230530130749713.csv",
#     sep="\t",
#     convert=True,
#     chunk_size=5_000_000,
# )
# %%
for i, df in enumerate(
    vaex.read_csv(
        "/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/0083459-230530130749713.csv",
        sep="\t",
        chunk_size=100_000,
    )
):
    df = df[
        [
            "kingdom",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
            "species",
            "decimalLatitude",
            "decimalLongitude",
            "license",
            "rightsHolder",
        ]
    ]
    df.export_parquet(
        f"/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/park/{i:02}.parquet"
    )
# df.head()
# %%
df.export_parquet(
    "/media/muskrat/T7 Shield/eco_data/v2/data_init/gbif_occurences/0083459-230530130749713.parquet"
)

# %%
# find the highest number in the coordinateUncertaintyInMeters column using vaex.max and store the result in a new variable
max_uncertainty_in_meters = df["coordinateUncertaintyInMeters"].max()

min_year = df["year"].min()


# %%
# create a new variable that only contains the columns kingdom, phylum, class, order, family, genus, species, decimalLatitude, decimalLongitude, license, and rightsHolder
df_essential = df[
    [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "decimalLatitude",
        "decimalLongitude",
        "license",
        "rightsHolder",
    ]
]

df_essential

# %%
# read df_essential into a parquet file where user can input the file path
df_essential_path = input("Enter the file path: ")
df_essential.export_parquet(df_essential_path)


# %%
# RELOAD AND RESTART FROM THIS POINT
df_essential_path = input("Enter the file path: ")

df_essential = vaex.open(df_essential_path)


# %%
df_essential

# %%
# count the number of na values in column species
species_na = df_essential["species"].isna().sum()

latitude_na = df_essential["decimalLatitude"].isna().sum()

longitude_na = df_essential["decimalLongitude"].isna().sum()


# %%

# remove rows in df_essential where species is NA
df_essential = df_essential.dropna(
    column_names=["species", "decimalLatitude", "decimalLongitude"]
)

df_essential

# %%
# recount the number of na values in column species
species_na = df_essential["species"].isna().sum()

latitude_na = df_essential["decimalLatitude"].isna().sum()

longitude_na = df_essential["decimalLongitude"].isna().sum()

# %%
# find unique species names in df_essential
species_unique = df_essential["species"].unique()

# %%
# read cleaned df_essential into a parquet file where user can input the file path
df_clean_path = input("Enter the file path: ")
df_essential.export_parquet(df_clean_path)

# %%
# pseudo code to check if a scientific name is longer than 2 words so we can remove it for now
# # # find number of words in each species row
# # species_words = df_essential['species'].str.split().str.len()

# # species_values = species_words.values

# species_words = df_essential['species'].to_numpy()


# # find number of words in each string in ndarray species_words
# # species_value = df_essential['species'].str.split().str.len()
# species_values = np.array([len(word.split()) for word in species_words])

# # find max value in species_values
# max_value = np.max(species_values)


# %%

# INATURALIST DATA

df = dd.read_csv(
    "/media/muskrat/060A9B8E0A9B78FF/inat_filtered/0061409-210914110416597.csv",
    sep="\t",
    dtype={
        "infraspecificEpithet": "object",
        "establishmentMeans": "object",
        "day": "float64",
        "month": "float64",
        "year": "float64",
    },
    assume_missing=True,
)


# %%


# df_head = df.head()
# df1 = df_head.loc[1,]
df.to_parquet("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park")
# names=['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'verbatimScientificName', 'decimalLatitude', 'decimalLongitute', 'coordinateUncertaintyInMeters', 'license', 'issue']


# %%


# pf = pd.read_csv('/media/muskrat/060A9B8E0A9B78FF/0053270-210914110416597/0053270-210914110416597.csv', nrows=200, index_col=False,  sep='\t' )

dp = dd.read_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park",
    columns=[
        "kingdom",
        "phylum",
        "class",
        "family",
        "order",
        "verbatimScientificName",
        "decimalLatitude",
        "decimalLongitude",
        "license",
        "rightsHolder",
        "issue",
    ],
)


# %%


# dp_head = dp.head()
dp.to_parquet("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park_rr")
# names=['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 've


# %%


# pp = pd.read_parquet('/media/muskrat/060A9B8E0A9B78FF/0053270-210914110416597/park_reduced')
pp = dd.read_parquet("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park_rr")


# %%


# print(pp.loc[dd.isna(pp['decimalLongitude']), :].index)


pp = dask_geopandas.from_dask_dataframe(pp)

pp = pp.set_geometry(
    dask_geopandas.points_from_xy(pp, "decimalLongitude", "decimalLatitude")
)

p_drop = pp.drop(["decimalLatitude", "decimalLongitude"], axis=1)

p_drop = p_drop.compute()

df = p_drop

del p_drop

del pp

df.rename(columns={"verbatimScientificName": "scientific_name"}, inplace=True)

df.to_csv("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/man/man.csv")

# p_drop.set_crs('epsg:4326')

# p_drop.to_parquet('/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park_crc')
# gdf = gpd.GeoDataFrame(pp, geometry=gpd.points_from_xy(pp.decimalLongitude, pp.decimalLatitude))


# %%


# gpd.options.use_pygeos = False
df = pd.read_csv("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/man/man.csv")

df = df.drop(["Unnamed: 0"], axis=1)

df.reset_index(drop=True, inplace=True)


# %%


df["kingdom"] = df["kingdom"].astype("category")

df["phylum"] = df["phylum"].astype("category")

df["class"] = df["class"].astype("category")

df["family"] = df["family"].astype("category")

df["order"] = df["order"].astype("category")

df["license"] = df["license"].astype("category")

df["rightsHolder"] = df["rightsHolder"].astype("category")

df["issue"] = df["issue"].astype("category")

df["scientific_name"] = df["scientific_name"].astype("category")


# %%


# missing = df.loc[df.isnull().any(axis=1)]

# un = df[df.issue.unique()]


# %%


# df = df[ (df['issue'] != 'COORDINATE_ROUNDED;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;TAXON_MATCH_HIGHERRANK;MULTIMEDIA_DATE_INVALID')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;PRESUMED_SWAPPED_COORDINATE')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;PRESUMED_NEGATED_LONGITUDE')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;PRESUMED_NEGATED_LATITUDE')]
# df = df[ (df['issue'] !='COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;PRESUMED_SWAPPED_COORDINATE;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_HIGHERRANK')]
# df = df[ (df['issue'] !='COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;PRESUMED_NEGATED_LATITUDE;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;TAXON_MATCH_FUZZY;MULTIMEDIA_DATE_INVALID')]
# df = df[ (df['issue'] !='COORDINATE_OUT_OF_RANGE')]
# df = df[ (df['issue'] !='TAXON_MATCH_HIGHERRANK;MULTIMEDIA_DATE_INVALID')]
# df = df[ (df['issue'] !='COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_FUZZY')]
# df = df[ (df['issue'] !='COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_NONE')]
# df = df[ (df['issue'] != 'TAXON_MATCH_FUZZY;MULTIMEDIA_DATE_INVALID')]


df.drop(
    df.loc[df["issue"] == "COORDINATE_ROUNDED;TAXON_MATCH_HIGHERRANK"].index,
    inplace=True,
)
df.drop(df.loc[df["issue"] == "TAXON_MATCH_HIGHERRANK"].index, inplace=True)
df.drop(
    df.loc[df["issue"] == "COORDINATE_ROUNDED;TAXON_MATCH_FUZZY"].index, inplace=True
)
df.drop(df.loc[df["issue"] == "TAXON_MATCH_FUZZY"].index, inplace=True)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;TAXON_MATCH_HIGHERRANK;MULTIMEDIA_DATE_INVALID"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_FUZZY"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COORDINATE_ROUNDED;PRESUMED_SWAPPED_COORDINATE"].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COORDINATE_ROUNDED;PRESUMED_NEGATED_LONGITUDE"].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COORDINATE_ROUNDED;PRESUMED_NEGATED_LATITUDE"].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"] == "COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;PRESUMED_SWAPPED_COORDINATE;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_FUZZY"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_FUZZY"
    ].index,
    inplace=True,
)


# %%


df.drop(
    df.loc[
        df["issue"] == "COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_HIGHERRANK"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_HIGHERRANK"].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"] == "COORDINATE_UNCERTAINTY_METERS_INVALID;TAXON_MATCH_FUZZY"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_FUZZY"].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"] == "COORDINATE_ROUNDED;PRESUMED_NEGATED_LATITUDE;TAXON_MATCH_FUZZY"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"] == "COORDINATE_ROUNDED;TAXON_MATCH_FUZZY;MULTIMEDIA_DATE_INVALID"
    ].index,
    inplace=True,
)
df.drop(df.loc[df["issue"] == "COORDINATE_OUT_OF_RANGE"].index, inplace=True)
df.drop(
    df.loc[df["issue"] == "TAXON_MATCH_HIGHERRANK;MULTIMEDIA_DATE_INVALID"].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "COUNTRY_COORDINATE_MISMATCH;TAXON_MATCH_FUZZY"].index,
    inplace=True,
)
df.drop(
    df.loc[
        df["issue"]
        == "COORDINATE_ROUNDED;COUNTRY_DERIVED_FROM_COORDINATES;TAXON_MATCH_NONE"
    ].index,
    inplace=True,
)
df.drop(
    df.loc[df["issue"] == "TAXON_MATCH_FUZZY;MULTIMEDIA_DATE_INVALID"].index,
    inplace=True,
)


# %%


df.reset_index(drop=True, inplace=True)


# %%


df.to_csv("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/geo/geo.csv", index=False)


# %%


df = pd.read_csv("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/geo/geo.csv")


# %%


df["kingdom"] = df["kingdom"].astype("category")

df["phylum"] = df["phylum"].astype("category")

df["class"] = df["class"].astype("category")

df["license"] = df["license"].astype("category")

df["rightsHolder"] = df["rightsHolder"].astype("category")

df["issue"] = df["issue"].astype("category")

df["scientific_name"] = df["scientific_name"].astype("category")


# %%


# pp = dask_geopandas.from_dask_dataframe(df)

# pp = pp[(pp['coordinateUncertaintyInMeters'] <= 400)]

# pp = pp.drop(['coordinateUncertaintyInMeters', 'issue'], axis=1)

# pp.to_parquet('/media/muskrat/060A9B8E0A9B78FF/0053270-210914110416597/man')

# # dp = pp[(pp[pp['geometry'].is_empty])]
# # df = df[(df['issue'] != 'TAXON_MATCH_FUZZY;MULTIMEDIA_DATE_INVALID')]

# df.drop(df[df['coordinateUncertaintyInMeters'] >= 400].index, inplace=True)

# df = df.drop(['coordinateUncertaintyInMeters', 'issue'], axis=1)

df["geometry"] = df["geometry"].apply(wkt.loads)

df = gpd.GeoDataFrame(df, crs="epsg:4326")

df.drop(df[df["geometry"].is_empty].index, inplace=True)


# df.drop(df[df['coordinateUncertaintyInMeters'] >= 400].index, inplace=True)

# df = df.drop(['coordinateUncertaintyInMeters', 'issue'], axis=1)


# dd = pd.read_parquet('/media/muskrat/060A9B8E0A9B78FF/0053270-210914110416597/man')


# %%


# df_head = df.head(n=10)

# df_tail = df.tail(n=1000)

# unique = df.drop_duplicates(['verbatimScientificName'])

rights = ["license", "rightsHolder"]
df["rights"] = df[rights].to_dict(orient="records")
df = df.drop(["license", "rightsHolder", "issue"], axis=1)


# %%


# print(df.loc[pd.isna(df['geometry']), :].index)


df.reset_index(drop=True, inplace=True)


# %%


# df.set_crs('EPSG:4326', inplace=True)

df.to_parquet("/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park_geo/park_geo.parquet")


# %%


df = gpd.read_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/inat_filtered/park_geo/park_geo.parquet"
)


# %%


ecomap = gpd.read_file("/home/muskrat/Documents/eco_data_copy/wwf_map_data/map.geojson")


# %%


# head = df.head(n=1000)

ecomap = ecomap.drop(
    [
        "OBJECTID",
        "AREA",
        "PERIMETER",
        "REALM",
        "BIOME",
        "ECO_NUM",
        "ECO_NAME",
        "ECO_ID",
        "ECO_SYM",
        "GBL_STAT",
        "G200_REGIO",
        "G200_NUM",
        "G200_BIOME",
        "G200_STAT",
        "Shape_Leng",
        "Shape_Area",
        "area_km2",
        "ECOREGION_CODE",
        "PER_area",
        "PER_area_1",
        "PER_area_2",
    ],
    axis=1,
)

ecomap["unique_id"] = ecomap["unique_id"].astype("float32")

ecomap.drop(ecomap[ecomap["unique_id"].isna()].index, inplace=True)

ecomap["unique_id"] = ecomap["unique_id"].astype("int32")

ecomap["unique_id"] = ecomap["unique_id"].apply(str)

ecomap["unique_id"] = ecomap["unique_id"].astype("category")


# ecomap.loc[4887, 'geometry'].contains(head.loc[5, 'geometry'])

# test = ecomap.sjoin(head, predicate='contains')
# test2 = head.sjoin(ecomap, predicate='contains')
# test3 = head.sjoin(ecomap, predicate='within')

# test3 = test3.drop(['index_right', 'ECO_NAME'], axis=1)

# ecos3 = test3.groupby('scientific_name')['unique_id'].apply(list).reset_index()


# %%


joined = df.sjoin(ecomap, predicate="within")


# %%


joined = joined.drop(["index_right", "geometry"], axis=1)


# %%


ecos3 = joined.groupby("scientific_name")["unique_id"].apply(list).reset_index()

ecos4 = joined.groupby("scientific_name")["rights"].apply(list).reset_index()

# ecos5 = joined.groupby(['scientific_name'], as_index=False)


# %%


# ecos5 = ecos5.apply(lambda x: x)
ecos7 = joined.drop_duplicates(subset=["scientific_name"])
ecos7 = ecos7.drop(["unique_id", "rights"], axis=1)


# %%


ecos6 = pd.merge(ecos3, ecos4, on="scientific_name", how="left").reindex(
    columns=["scientific_name", "unique_id", "rights"]
)


final = pd.merge(ecos6, ecos7, on="scientific_name", how="left")

final1 = final[final["unique_id"].map(lambda d: len(d)) > 0]

final2 = final1[
    [
        "kingdom",
        "phylum",
        "class",
        "family",
        "order",
        "scientific_name",
        "unique_id",
        "rights",
    ]
]


# %%


finalj = final2.to_json(orient="records", force_ascii=False)
repr(finalj)


# %%


file = open("inat_full.json", "w")
file.write(finalj)
file.close
