# %%
import pandas as pd
import os


pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_seq_items', 100)

# %%
# create a list of all files in the directory
base_path = input("Enter the file base path: ")
files = os.listdir(base_path)

# %%
final_path = input("Enter the final file path: ")


# %%
def merge_columns(row):
    if pd.isnull(row["rightsHolder"]):
        return row["rightsHolder"]

    else:
        if pd.isnull(row["license"]):
            return row["rightsHolder"] + ", "

        else:
            return row["rightsHolder"] + ", " + row["license"]


# %%
geodetic_datum = [
    "EPSG:4326",
    "EPSG:4326 WGS84",
    "EPSG: 4326 WGS 84",
    "WGS84_EPSG:4326",
    "EPSG:4326. WGS 84",
    "wgs84",
    "EPSG:4326 (WGS84)",
    "WGS-84",
    "EPSG:4326 - WGS 84",
    "World Geodetic System 1984",
    "ESPG:4326 WGS84 decimal degrees",
    "EPSG:4326 WGS 84",
    "WGS 84",
    "WGS84",
]

license_removal = [
    "Copyright Russian Cetacean Habitat Project. No use without permission.",
    "Copyright Jodi Frediani. No use without permission.",
    "Copyright Rémi Bigonneau. No use without permission.",
    "Copyright Tasli Shaw. No use without permission.",
    "Copyright Adam A. Pack. No use without permission.",
    "Copyright Whales of Guerrero (contact: Katherina Audley). No use without permission.",
    "Copyright Nico Ransome. No use without permission.",
    "Copyright Sea Of Whales Adventures. No use without permission.",
    "Copyright Olaf Meynecke. No use without permission.",
    "Copyright Marcel V. de Morais. No use without permission.",
    "Copyright Vallarta Adventures. No use without permission.",
    "Copyright OMMAG. No use without permission.",
    "Copyright Ma. Eugenia Rodríguez Vázquez. No use without permission.",
    "Copyright Turks and Caicos Islands Whale Project. No use without permission.",
    "Copyright Leanne Maffesoni. No use without permission.",
    "Copyright Manami Yamaguchi. No use without permission.",
    "Copyright Allied Whale North Atlantic Humpback Whale Catalog. No use without permission.",
    "Copyright Frank Garita Alpízar. No use without permission.",
]

# %%
for file in files:
    print(file)

    df = pd.read_parquet(f"{base_path}/{file}")

    df = df.drop(
        columns=[
            "flags",
        ]
    )

    df = df.dropna(
        subset=[
            "decimalLatitude",
            "decimalLongitude",
            "coordinateUncertaintyInMeters",
            "species",
            "basisOfRecord",
        ]
    )

    # convert column coordinateUncertaintyInMeters to float
    df["coordinateUncertaintyInMeters"] = df["coordinateUncertaintyInMeters"].astype(
        "float32"
    )

    # fill na date_year with 0
    df["date_year"] = df["date_year"].fillna(0)

    df["date_year"] = df["date_year"].astype("int32")

    df["decimalLatitude"] = df["decimalLatitude"].astype("float32")
    df["decimalLongitude"] = df["decimalLongitude"].astype("float32")

    df = df[df.coordinateUncertaintyInMeters <= 400]

    max_uncertainty_in_meters = df["coordinateUncertaintyInMeters"].max()
    min_uncertainty_in_meters = df["coordinateUncertaintyInMeters"].min()
    print(
        f"Max uncertainty: {max_uncertainty_in_meters}, Min uncertainty: {min_uncertainty_in_meters}"
    )

    df = df[df.date_year >= 1924]

    min_year = df["date_year"].min()
    max_year = df["date_year"].max()
    print(f"Min year: {min_year}, Max year: {max_year}")

    max_lat = df["decimalLatitude"].max()
    min_lat = df["decimalLatitude"].min()

    max_long = df["decimalLongitude"].max()
    min_long = df["decimalLongitude"].min()

    print(f"Max latitude: {max_lat}, Min latitude: {min_lat}")
    print(f"Max longitude: {max_long}, Min longitude: {min_long}")

    df = df[
        df.basisOfRecord.isin(
            [
                "HumanObservation",
                "MachineObservation",
                "Occurrence",
                "Human observation",
                "machineObservation",
                "humanobservation",
                "Humanobservation",
                "humanObservation",
                "Human Observation",
                "machineobservation",
            ]
        )
    ]

    df = df[~df.license.isin(license_removal)]

    df = df[(df.geodeticDatum.isin(geodetic_datum)) | (df.geodeticDatum.isna())]

    df["rights"] = None

    df["rights"] = df.apply(merge_columns, axis=1)

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
            "rights",
        ]
    ]

    df.to_parquet(f"{final_path}/{file}")
