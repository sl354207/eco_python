import geopandas as gpd
import pandas as pd

import os

os.environ["PROJ_LIB"] = "/home/muskrat/miniconda3/envs/eco/share/proj"

# %%
zip = input("Enter the zip file path: ")
ecomap_loc = input("Enter the ecomap file path: ")
final_base = input("Enter the final base path: ")

# %%

df = gpd.read_file(zip)

eco_map = gpd.read_file(ecomap_loc)

# %%
# some are Shape_Leng and Shape_Area instead of SHAPE_Leng and SHAPE_Area
# if df contains columns SHAPE_Leng and SHAPE_Area, rename them to Shape_Leng and Shape_Area
if "SHAPE_Leng" in df.columns:
    df = df.rename(columns={"SHAPE_Leng": "Shape_Leng"})

if "SHAPE_Area" in df.columns:
    df = df.rename(columns={"SHAPE_Area": "Shape_Area"})


df = df.drop(
    columns=[
        "id_no",
        "seasonal",
        "compiler",
        "yrcompiled",
        "subspecies",
        "subpop",
        "Shape_Leng",
        "Shape_Area",
        "source",
        "island",
        "tax_comm",
        "dist_comm",
        "generalisd",
        "legend",
        "citation",
        "category",
        "marine",
        "terrestial",
        "freshwater",
        "kingdom",
        "phylum",
        "class",
        "order_",
        "family",
        "genus",
    ]
)

# remove rows of df where column 'presence' is not 1 or 2
df = df[df["presence"].isin([1, 2])]

# remove rows of df where column 'origin' is not 1
df = df[df["origin"].isin([1])]

df = df.to_crs("EPSG:4326")

unique_species = df["sci_name"].unique().tolist()

# remove items in unique_species that contain more than 2 words
unique_species = [x for x in unique_species if len(x.split()) < 3]

print(len(unique_species))
# %%
# sort unique_species alphabetically for restarts if not processing whole file at once
unique_species.sort()
# %%

# FW_FISH_2 - 6889
for i in range(1500, 2000, 1):
    # for i in range(3500, len(unique_species), 1):
    species = unique_species[i]
    # for species in unique_species:
    species_df = df[df["sci_name"] == species]

    overlay = gpd.overlay(species_df, eco_map, how="intersection")

    unique_ids = list(overlay["unique_id"].unique())

    eco_map_unique = eco_map[eco_map["unique_id"].isin(unique_ids)]

    # remove rows with <NA> in unique_id from eco_map_unique

    eco_map_unique = eco_map_unique[eco_map_unique["unique_id"] != "<NA>"]

    overlay = overlay[overlay["unique_id"] != "<NA>"]

    overlay["area"] = overlay.geometry.area

    overlay_areas = overlay[["unique_id", "area"]].groupby("unique_id").sum()

    eco_map_unique["area"] = eco_map_unique.geometry.area

    eco_map_unique_areas = (
        eco_map_unique[["unique_id", "area"]].groupby("unique_id").sum()
    )

    combined_areas = pd.concat([overlay_areas, eco_map_unique_areas], axis=1)
    combined_areas.columns = ["overlay_area", "eco_map_unique_area"]

    combined_areas["ratio"] = (
        combined_areas["overlay_area"] / combined_areas["eco_map_unique_area"]
    )

    # keep ecoregion if only present in 1 area
    if len(combined_areas) == 1:
        native = unique_ids
    else:
        # if ratio > 0.2 then add unique_id to list of ids
        native = combined_areas[combined_areas["ratio"] > 0.2].index.tolist()

        if len(native) == 0:

            native = combined_areas[
                combined_areas["eco_map_unique_area"] <= 2
            ].index.tolist()

            if len(native) == 0:
                native = combined_areas[combined_areas["ratio"] > 0.1].index.tolist()

                if len(native) == 0:
                    native = ["check"]

    scientific_name = species_df["sci_name"].values[0]

    species_final = pd.DataFrame(
        {
            "scientific_name": scientific_name,
            "database": "iucn",
            "unique_id": native,
        },
    )

    # groupby scientific_name
    species_final = (
        species_final.groupby(["scientific_name", "database"])["unique_id"]
        .apply(list)
        .reset_index()
    )

    # native_df = pd.DataFrame({"sci_name": scientific_name, "unique_id": native})

    # native_df = native_df.groupby(["sci_name"])["unique_id"].apply(list).reset_index()

    # species_final = pd.merge(species_df, native_df, on="sci_name")

    # species_final = species_final.drop(columns=["presence", "origin", "geometry"])

    # species_final = species_final.rename(
    #     columns={"sci_name": "scientific_name", "order_": "order"}
    # )

    species_final.to_parquet(
        final_base + "/" + scientific_name.replace(" ", "_") + ".parquet"
    )

    # final = pd.concat([final, species_final], axis=0)
# %%

# final.to_parquet(final_path)
