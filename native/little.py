import geopandas as gpd
import pandas as pd

# %%
meta_input = input("Enter the metadata file path: ")

metadata = pd.read_csv(
    meta_input,
)
# %%
# restart from error row if necessary
row = metadata[metadata["SHP/*"] == "querilic"].index[0]

metadata = metadata.iloc[row:, :]
# %%
# create new dataframe with latin names, common names, and shp/* columns from metadata

metadata = metadata[["Latin Name", "Common Name", "SHP/*"]]
# %%

ecomap_loc = input("Enter the ecomap file path: ")

eco_map = gpd.read_file(ecomap_loc)

# %%

little_base = input("Enter the little base file path: ")

final_base_path = input("Enter the final base path: ")

for shp in metadata["SHP/*"]:
    print(shp)
    path = f"{little_base}{shp}.geojson"

    little = gpd.read_file(
        path,
    )

    # convert crs
    little = little.to_crs("EPSG:4326")

    # find intersecting geometry
    intersects = gpd.sjoin(little, eco_map)

    # put unique values of unique_id in intersects into a list
    unique_ids = list(intersects["unique_id"].unique())

    # create dataframe from eco_map that only contains the unique ids in unique_ids
    eco_map_unique = eco_map[eco_map["unique_id"].isin(unique_ids)]

    # remove rows with <NA> in unique_id from eco_map_unique
    eco_map_unique = eco_map_unique[eco_map_unique["unique_id"] != "<NA>"]

    # remove marine ecoregions
    eco_map_unique = eco_map_unique[~eco_map_unique["TYPE"].isin(["MEOW", "PPOW"])]

    # find overlaying geometry
    overlay = gpd.overlay(little, eco_map, how="intersection")

    # remove rows with <NA> in unique_id from overlay
    overlay = overlay[overlay["unique_id"] != "<NA>"]

    # remove marine ecoregions
    overlay = overlay[~overlay["TYPE"].isin(["MEOW", "PPOW"])]

    # add area column to overlay
    overlay["area"] = overlay.geometry.area

    # create a new dataframe from overlay where the first column is unique_id and the second column is the area of all the rows in overlay that have the same unique_id
    overlay_areas = overlay[["unique_id", "area"]].groupby("unique_id").sum()

    # add an area column to eco_map_unique dataframe
    eco_map_unique["area"] = eco_map_unique.geometry.area

    # create a new dataframe from eco_map_unique where the first column is unique_id and the second column is the area of all the rows in eco_map_unique that have the same unique_id
    eco_map_unique_areas = (
        eco_map_unique[["unique_id", "area"]].groupby("unique_id").sum()
    )

    # combine eco_map_unique_areas and overlay_areas into a new dataframe where the first column is unique_id, the second column is area from overlays, and the third column is area from eco_map_unique
    combined_areas = pd.concat([overlay_areas, eco_map_unique_areas], axis=1)
    combined_areas.columns = ["overlay_area", "eco_map_unique_area"]

    # keep ecoregion if only present in 1 area
    if len(combined_areas) == 1:
        native = unique_ids
    else:
        # if overlay_area / eco_map_unique_area > 0.2 then add unique_id to list of ids
        native = combined_areas[
            combined_areas["overlay_area"] / combined_areas["eco_map_unique_area"] > 0.2
        ].index.tolist()

        if len(native) == 0:

            native = combined_areas[
                combined_areas["eco_map_unique_area"] <= 2
            ].index.tolist()

            if len(native) == 0:
                native = combined_areas[
                    combined_areas["overlay_area"]
                    / combined_areas["eco_map_unique_area"]
                    > 0.1
                ].index.tolist()

                if len(native) == 0:
                    native = ["check"]

    # # convert to df
    # native_df = eco_map[eco_map["unique_id"].isin(native)]

    # scientific_name equals the Latin Name in metadata at value of SHP/*
    scientific_name = metadata.loc[metadata["SHP/*"] == shp]["Latin Name"].values[0]

    common_name = metadata.loc[metadata["SHP/*"] == shp]["Common Name"].values[0]
    # final dataframe
    final = pd.DataFrame(
        {
            "scientific_name": scientific_name,
            "common_name": common_name,
            "unique_id": native,
            "database": "little",
        },
    )

    # groupby scientific_name
    final = (
        final.groupby(["scientific_name", "common_name"])["unique_id"]
        .apply(list)
        .reset_index()
    )

    string_name = str(final["scientific_name"].values[0]).replace(" ", "_")

    path = final_base_path + string_name + ".parquet"

    final.to_parquet(path)


# %%
