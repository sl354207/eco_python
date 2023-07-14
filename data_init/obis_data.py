# %%
import pandas as pd
import geopandas as gpd
import vaex
import numpy as np

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

# %%
df_path = input("Enter the file path: ")

df = vaex.open(df_path)

# %%
df.describe()

# %%
# create a new variable that only contains the columns kingdom, phylum, class, order, family, genus, species, decimalLatitude, decimalLongitude, license, and rightsHolder
df_essential = df[['kingdom','phylum','class','order','family','genus','species','decimalLatitude','decimalLongitude','license','rightsHolder','geodeticDatum','date_year','basisOfRecord','flags','coordinateUncertaintyInMeters']]

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
df_essential.describe()

# %%
df_essential = df_essential.dropna(column_names=['decimalLatitude', 'decimalLongitude', 'coordinateUncertaintyInMeters', 'geodeticDatum', 'species', 'date_year', 'basisOfRecord' ])

# %%
df_essential.describe()

# %%
df_essential.geodeticDatum.value_counts()

# %%
# keep rows where geodeticDatum is EPSG:4326, EPSG:4326 WGS84, or WGS84_EPSG:4326
df_essential = df_essential[df_essential.geodeticDatum.isin(['EPSG:4326', 'EPSG:4326 WGS84', 'EPSG: 4326 WGS 84', 'WGS84_EPSG:4326'])]

# %%
df_essential.describe()

# %%
df_essential.scientificName.unique().len()
df_essential.species.unique().len()

# %%
# create a new column that displays the word count of the scientificName column
# species_words = df_essential['scientificName'].to_numpy()
# species_value = df_essential['scientificName'].str.split().str.len()
# species_values = np.array([len(word.split()) for word in species_words])

# df_essential['word_length'] = df_essential['scientificName'].str.split().str.len()

# %%
# get length of value in the word_length column



# %%
# create a new column tht displays the length of the word_length column
# df_essential['word_count'] = np.array([len(word) for word in df_essential['word_length']]) 

# %%
df.describe()
df.head()
df.tail()
df.column_names
df.drop(["AphiaID", "institutionCode"], inplace=True)

df.drop(
    [
        "minimumDepthInMeters",
        "maximumDepthInMeters",
        "shoredistance",
        "bathymetry",
        "sst",
        "sss",
        "marine",
        "brackish",
        "freshwater",
        "terrestrial",
        "taxonRank",
        "redlist_category",
        "superdomain",
        "domain",
        "subspecies",
        "natio",
        "variety",
        "subvariety",
        "forma",
        "subforma",
        "institutionID",
        "collectionID",
        "datasetID",
        "collectionCode",
        "datasetName",
        "ownerInstitutionCode",
        "informationWithheld",
        "dataGeneralizations",
        "dynamicProperties",
        "materialSampleID",
        "occurrenceID",
        "catalogNumber",
        "occurrenceRemarks",
        "recordNumber",
        "recordedBy",
        "recordedByID",
        "individualCount",
        "organismQuantity",
        "organismQuantityType",
        "sex",
        "lifeStage",
        "reproductiveCondition",
        "behavior",
        "occurrenceStatus",
        "preparations",
        "disposition",
        "otherCatalogNumbers",
        "associatedMedia",
        "associatedReferences",
        "associatedSequences",
        "associatedTaxa",
        "organismID",
        "organismName",
        "organismScope",
        "associatedOccurrences",
        "associatedOrganisms",
        "previousIdentifications",
        "organismRemarks",
        "eventID",
        "parentEventID",
        "samplingProtocol",
        "sampleSizeValue",
        "sampleSizeUnit",
        "verbatimEventDate",
        "habitat",
        "fieldNumber",
        "fieldNotes",
        "eventRemarks",
        "locationID",
        "higherGeographyID",
        "higherGeography",
        "continent",
        "waterBody",
        "islandGroup",
        "island",
        "country",
        "countryCode",
        "stateProvince",
        "county",
        "municipality",
        "locality",
        "verbatimLocality",
        "verbatimElevation",
        "minimumElevationInMeters",
        "maximumElevationInMeters",
        "verbatimDepth",
        "minimumDistanceAboveSurfaceInMeters",
        "maximumDistanceAboveSurfaceInMeters",
        "locationAccordingTo",
        "locationRemarks",
        "geologicalContextID",
        "earliestEonOrLowestEonothem",
        "latestEonOrHighestEonothem",
        "earliestEraOrLowestErathem",
        "latestEraOrHighestErathem",
        "earliestPeriodOrLowestSystem",
        "latestPeriodOrHighestSystem",
        "earliestEpochOrLowestSeries",
        "latestEpochOrHighestSeries",
        "earliestAgeOrLowestStage",
        "latestAgeOrHighestStage",
        "lowestBiostratigraphicZone",
        "highestBiostratigraphicZone",
        "lithostratigraphicTerms",
        "group",
        "formation",
        "member",
        "bed",
        "identificationID",
        "identifiedBy",
        "identifiedByID",
        "dateIdentified",
        "identificationReferences",
        "identificationRemarks",
        "identificationQualifier",
        "identificationVerificationStatus",
        "typeStatus",
        "taxonID",
        "scientificNameID",
        "acceptedNameUsageID",
        "parentNameUsageID",
        "originalNameUsageID",
        "nameAccordingToID",
        "namePublishedInID",
        "taxonConceptID",
        "acceptedNameUsage",
        "parentNameUsage",
        "originalNameUsage",
        "nameAccordingTo",
        "namePublishedIn",
        "namePublishedInYear",
        "higherClassification",
        "specificEpithet",
        "infraspecificEpithet",
        "verbatimTaxonRank",
        "scientificNameAuthorship",
        "nomenclaturalCode",
        "taxonomicStatus",
        "nomenclaturalStatus",
    ],
    inplace=True,
)

df.drop(
    [
        "dataset_id",
        "subkingdom",
        "infrakingdom",
        "phylum_division",
        "subphylum_subdivision",
        "subphylum",
        "infraphylum",
        "parvphylum",
        "gigaclass",
        "megaclass",
        "superclass",
        "subclass",
        "infraclass",
        "subterclass",
        "superorder",
        "suborder",
        "infraorder",
        "parvorder",
        "superfamily",
        "subfamily",
        "supertribe",
        "tribe",
        "subtribe",
        "subgenus",
        "section",
        "subsection",
        "series",
        "samplingEffort",
        "pointRadiusSpatialFit",
        "footprintSpatialFit",
    ],
    inplace=True,
)

df.drop(
    [
        "date_start",
        "date_mid",
        "date_end",
        "originalScientificName",
        "language",
        "eventDate",
        "eventTime",
        "startDayOfYear",
        "endDayOfYear",
        "month",
        "day",
    ],
    inplace=True,
)


# %%


df.export_parquet("/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_init/obis.parquet")


# %%


df = vaex.open("/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_init/obis.parquet")


# %%


df.drop(
    [
        "verbatimCoordinates",
        "verbatimLatitude",
        "verbatimLongitude",
        "verbatimCoordinateSystem",
        "verbatimSRS",
        "georeferenceSources",
        "georeferenceVerificationStatus",
    ],
    inplace=True,
)


# %%


df_year = df[df.date_year > 2000]
df_year
df_year.describe()


# %%


df_year.drop(["establishmentMeans"], inplace=True)
df_year.column_names

df_year.drop(["y", "year"], inplace=True)


# %%


df_year.export_csv("/media/muskrat/060A9B8E0A9B78FF/obis/vaex_csv_year/obis_year.csv")


# %%


df_year.export_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_year/obis_year.parquet"
)


# %%


vee = vaex.open("/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_year/obis_year.parquet")


# %%


vee.describe()

vee.dropped.value_counts()

vee.absence.value_counts()

vee.drop(["dropped", "absence"], inplace=True)

vee["type"].value_counts()

vee.drop(["type"], inplace=True)

vee.modified.value_counts()

vee.drop(["modified"], inplace=True)

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

vee.drop(
    [
        "footprintWKT",
        "footprintSRS",
        "georeferencedBy",
        "georeferencedDate",
        "georeferenceProtocol",
        "georeferenceRemarks",
        "taxonRemarks",
        "references",
    ],
    inplace=True,
)


# %%


vee["coord"] = vee.coordinateUncertaintyInMeters.astype("float32")

vee_meter = vee[vee.coord < 390]

vee_meter.describe()

vee_meter.scientificName.value_counts()

vee_meter.drop(["coord"], inplace=True)


# %%


vee_meter.export_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_geo/obis_geo.parquet"
)


# %%


obis = pd.read_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/obis/vaex_park_geo/obis_geo.parquet"
)


# %%


obis.dropna(subset=["species"], inplace=True)

science = obis[["scientificName"]]
species = obis[["species"]]

science.rename(columns={"scientificName": "species"}, inplace=True)

obis.reset_index(drop=True, inplace=True)


# %%


obis.drop(obis.loc[obis["geodeticDatum"] == "NAD83"].index, inplace=True)

obis.drop(obis.loc[obis["geodeticDatum"] == "unknown"].index, inplace=True)

obis.drop(obis.loc[obis["geodeticDatum"] == "not recorded"].index, inplace=True)

obis.drop(obis.loc[obis["geodeticDatum"] == "AGD 66"].index, inplace=True)


# %%


obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright Glacier Bay National Park Humpback Whale Monitoring Program. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright Russian Cetacean Habitat Project. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright Whales of Guerrero (contact: Katherina Audley). No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright Ma. Eugenia Rodríguez Vázquez. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Nico Ransome. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright IWC Southern Ocean Whale and Ecosystem Research (SOWER) cruises. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"]
        == "Copyright IWC Pacific Ocean Whale and Ecosystem Research (POWER) cruises. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Joelle De Weerdt. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Tasli Shaw. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Amy Kennedy. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright David Paton. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Amy Engelhaupt. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[obis["license"] == "Copyright HDR . No use without permission."].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Leanne Maffesoni. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Mithriel MacKay. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Ran Dembo. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[obis["license"] == "Copyright OMMAG. No use without permission."].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Krista Rossow. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Olaf Meynecke. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Daniel Burns. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright ralph pace. No use without permission."
    ].index,
    inplace=True,
)

obis.drop(
    obis.loc[
        obis["license"] == "Copyright Tory Kallman. No use without permission."
    ].index,
    inplace=True,
)

obis.reset_index(drop=True, inplace=True)


# %%


coord = obis[["scientificName"]].value_counts()

coord2 = obis[["species"]].value_counts()

coord = pd.DataFrame(coord)

coord.reset_index(inplace=True)

coord2 = pd.DataFrame(coord2)

coord2.reset_index(inplace=True)

coord.rename(columns={"scientificName": "species"}, inplace=True)

inner = pd.merge(coord, coord2, how="outer", on="species", indicator=True)

obis["scientific_name"] = obis["scientificName"]

obis["scientific_name"].fillna(obis["species"], inplace=True)

# obis['scientific_name'].isna().sum()


# %%


obis = obis.drop(
    [
        "coordinateUncertaintyInMeters",
        "coordinatePrecision",
        "date_year",
        "id",
        "basisOfRecord",
        "geodeticDatum",
        "flags",
        "scientificName",
        "species",
    ],
    axis=1,
)


# %%


obis["scientific_name"] = obis["scientific_name"].str.replace(r" \(.*\)", "")

tester = obis

tester[[0, 1, 2, 3, 4, 5]] = tester["scientific_name"].str.split(" ", expand=True)


tester.drop(tester[tester[1].isna()].index, inplace=True)

tester.drop(tester[tester[2].notna()].index, inplace=True)

tester.drop(tester[tester[3].notna()].index, inplace=True)

tester.drop(tester[tester[4].notna()].index, inplace=True)

tester.drop(tester[tester[5].notna()].index, inplace=True)

tester["scientific_name"] = tester[0].str.cat(tester[1], sep=" ")

tester["genus"] = tester[0]

tester = tester.drop([0, 1, 2, 3, 4, 5], axis=1)


# %%


tester.rename(columns={"vernacularName": "common_name"}, inplace=True)

obis = tester.drop(["accessRights"], axis=1)


# %%


obis["rightsHolder"].fillna(obis["bibliographicCitation"], inplace=True)
rights = ["license", "rightsHolder"]
obis["rights"] = obis[rights].to_dict(orient="records")
obis = obis.drop(["license", "rightsHolder", "bibliographicCitation"], axis=1)


# %%


obis.to_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/obis/obis_park_filtered/obis_filtered.parquet"
)


# %%


pp = pd.read_parquet(
    "/media/muskrat/060A9B8E0A9B78FF/obis/obis_park_filtered/obis_filtered.parquet"
)


# %%


gdf = gpd.GeoDataFrame(
    pp, geometry=gpd.points_from_xy(pp.decimalLongitude, pp.decimalLatitude)
)

gdf = gdf.drop(["decimalLatitude", "decimalLongitude"], axis=1)

df = gdf

df = gpd.GeoDataFrame(df, crs="epsg:4326")

df.drop(df[df["geometry"].is_empty].index, inplace=True)


# %%


marinemap = gpd.read_file(
    "/media/muskrat/060A9B8E0A9B78FF/eco_data/marine_map_data/marine_map.geojson"
)


# %%


joined = df.sjoin(marinemap, predicate="within")


# %%


joined = joined.drop(["index_right", "geometry"], axis=1)
