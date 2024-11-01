# %%
import pandas as pd
import os

pd.set_option("display.max_columns", None)


# %%
base_path = input("Enter the file base path: ")

final_path = input("Enter the final file path: ")


# %%
files = os.listdir(base_path)

# %%
# ECOREGIONS
# for file in files:
#     df = pd.read_parquet(f"{base_path}/{file}")

#     df = df[
#         [
#             "kingdom",
#             "phylum",
#             "class",
#             "order",
#             "family",
#             "genus",
#             "species",
#             "decimalLatitude",
#             "decimalLongitude",
#             "rights",
#         ]
#     ]

#     df.to_parquet(f"{final_path}/{file}")

# FRESHWATER
# for file in files:
#     df = pd.read_parquet(f"{base_path}/{file}")

#     df = df[
#         [
#             "kingdom",
#             "phylum",
#             "class",
#             "order",
#             "family",
#             "genus",
#             "species",
#             "decimalLatitude",
#             "decimalLongitude",
#             "countryCode",
#         ]
#     ]

#     df.to_parquet(f"{final_path}/{file}")

# SOIL
for file in files:
    df = pd.read_parquet(f"{base_path}/{file}")

    df = df[df["coordinateUncertaintyInMeters"] <= 20]

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
            "countryCode",
        ]
    ]

    df.to_parquet(f"{final_path}/{file}")
