# %%
import geopandas as gpd
import pandas as pd

# %%
ecomap_loc = input("Enter the ecomap file path: ")

eco_map = gpd.read_file(ecomap_loc)

# %%
gap_loc = input("Enter the gap file path: ")

gap = gpd.read_parquet(
    gap_loc,
)

# %%
final_path = input("Enter the final file path: ")
# %%
gap = gap.to_crs("EPSG:4326")


# %%
species_unique = gap["scientific_name"].unique().tolist()


# %%
final = pd.DataFrame()
for species in species_unique:
    species_df = gap[gap["scientific_name"] == species]

    # find intersecting geometry
    intersects = gpd.sjoin(species_df, eco_map)

    # put unique values of unique_id in intersects into a list
    unique_ids = list(intersects["unique_id"].unique())

    # create dataframe from eco_map that only contains the unique ids in unique_ids
    eco_map_unique = eco_map[eco_map["unique_id"].isin(unique_ids)]

    # remove rows with <NA> in unique_id from eco_map_unique
    eco_map_unique = eco_map_unique[eco_map_unique["unique_id"] != "<NA>"]

    # remove marine ecoregions
    eco_map_unique = eco_map_unique[~eco_map_unique["TYPE"].isin(["MEOW", "PPOW"])]

    # find overlaying geometry
    overlay = gpd.overlay(species_df, eco_map, how="intersection")

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

    scientific_name = species_df["scientific_name"].values[0]

    # final dataframe
    species_final = pd.DataFrame(
        {
            "scientific_name": scientific_name,
            "database": "gap",
            "unique_id": native,
        },
    )

    # groupby scientific_name
    species_final = (
        species_final.groupby(["scientific_name", "database"])["unique_id"]
        .apply(list)
        .reset_index()
    )

    final = pd.concat([final, species_final], axis=0)


# %%

final.to_parquet(final_path)
