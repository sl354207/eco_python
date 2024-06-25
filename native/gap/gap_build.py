import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import re
import os

import warnings

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# %%
# for i in range(0, 1719, 1):
#     # loading the temp.zip and creating a zip object
#     with ZipFile(
#         f"/media/muskrat/T7 Shield/eco_data/v3/native/GAP/scraped/{i}.zip", "r"
#     ) as zObject:

#         # Extracting all the members of the zip
#         # into a specific location.
#         zObject.extractall(
#             path=f"/media/muskrat/T7 Shield/eco_data/v3/native/GAP/unzipped/{i}"
#         )

# %%


file_list = []
# 500 1000 1500 1719
for i in range(0, 1719, 1):
    #  list the files inside directory {i}
    for file_path in os.listdir(f"/UPDATE_PATH/GAP/unzipped/{i}"):
        # print(file_path)
        # store each file path before extension in a list

        file_split = file_path.split(".")
        file_list.append(file_split[0])

# remove duplicates from file_list and preserve order
file_list = list(dict.fromkeys(file_list))

# %%
meta_df = pd.DataFrame()
name_df = pd.DataFrame()
geo_df = pd.DataFrame()
for i in range(0, 1719, 1):

    print(i)
    tree = ET.parse(f"/UPDATE_PATH/GAP/unzipped/{i}/{file_list[i]}.xml")
    root = tree.getroot()

    name = ""
    for title in root.iter("title"):
        if title.text != None:
            # print(title.text)

            text = title.text
            # find words contained inside parentheses and add to a variable
            if ("(" in text) and (")" in text):
                text = re.findall(r"\((.*?)\)", text)
                name = text[0]

            else:
                print("no name")

            break
        else:
            print("no title")
    zip = f"/UPDATE_PATH/GAP/unzipped/{i}/{file_list[i]}.zip"
    with ZipFile(zip, "r") as zObject:
        # print(zObject.namelist())
        # open file in zObject that ends in .csv and save to dataframe
        for j in zObject.namelist():
            if j.endswith(".csv"):
                # print(j)
                metadata = pd.read_csv(zObject.open(j))
                metadata["join"] = i
                meta_df = pd.concat([meta_df, metadata])
                meta_df = meta_df.drop(columns="strHUC12RNG")
                meta_df = meta_df.drop_duplicates()
                break

    # print(name)

    name_df = pd.concat(
        [name_df, pd.DataFrame({"scientific_name": [name], "join": [i]})]
    )

    geodata = gpd.read_file(zip)
    geodata["join"] = i
    # drop season code column and season name column from df

    geodata = geodata.drop(columns=["SeasonCode", "SeasonName"])

    geo_df = pd.concat([geo_df, geodata])

    # merge meta_df, name_df, and geo_df on join column

    df = pd.merge(meta_df, name_df, on="join")

    df = pd.merge(df, geo_df, on="join")

    df = df.drop(columns="join")

    # %%
    df = gpd.GeoDataFrame(df, geometry="geometry")
    # %%
step = 500
step_end = len(df)
for i in range(0, len(df), step):
    # split df into chunks of size 1000
    if i + step < step_end:
        sub = df.iloc[i : i + step]
        sub.to_parquet(f"/UPDATE_PATH/GAP/init/{i}.parquet")
    else:
        sub = df.iloc[i:]
        sub.to_parquet(f"/UPDATE_PATH/GAP/init/{i}.parquet")

        break
