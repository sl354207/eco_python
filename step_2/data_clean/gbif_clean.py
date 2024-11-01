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
        return row["rightsHolder"] + ", " + row["license"]


# %%
for file in files:
    print(file)

    df = pd.read_parquet(f"{base_path}/{file}")

    df = df.dropna(
        subset=[
            "species",
            "decimalLatitude",
            "decimalLongitude",
            "coordinateUncertaintyInMeters",
        ]
    )

    # convert column coordinateUncertaintyInMeters to float
    df["coordinateUncertaintyInMeters"] = df["coordinateUncertaintyInMeters"].astype(
        "float64"
    )

    df["year"] = df["year"].astype("int64")

    df["decimalLatitude"] = df["decimalLatitude"].astype("float64")
    df["decimalLongitude"] = df["decimalLongitude"].astype("float64")

    max_uncertainty_in_meters = df["coordinateUncertaintyInMeters"].max()
    min_uncertainty_in_meters = df["coordinateUncertaintyInMeters"].min()
    print(
        f"Max uncertainty: {max_uncertainty_in_meters}, Min uncertainty: {min_uncertainty_in_meters}"
    )

    min_year = df["year"].min()
    max_year = df["year"].max()
    print(f"Min year: {min_year}, Max year: {max_year}")

    max_lat = df["decimalLatitude"].max()
    min_lat = df["decimalLatitude"].min()

    max_long = df["decimalLongitude"].max()
    min_long = df["decimalLongitude"].min()

    print(f"Max latitude: {max_lat}, Min latitude: {min_lat}")
    print(f"Max longitude: {max_long}, Min longitude: {min_long}")

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
            "coordinateUncertaintyInMeters",
            "year",
            "countryCode",
        ]
    ]

    df.to_parquet(f"{final_path}/{file}")
