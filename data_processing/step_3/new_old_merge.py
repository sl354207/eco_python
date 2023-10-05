# %%
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_seq_items', 100)

# 400 meter coordinate accuracy
# 1923 earliest year


# %%
# read in new merged gbif_obis data
new_path = input("Enter the file path: ")

new = pd.read_parquet(new_path)


# %%
# read in old species data
old_path = input("Enter the file path: ")

# read in the old species data
old = pd.read_parquet(old_path)

# %%
del new_path, old_path

# %%
inner = pd.merge(new, old, how="inner", on=["scientific_name"], indicator=True)

# %%
outer = pd.merge(new, old, how="outer", on=["scientific_name"], indicator=True)

del new, old


# %%
inner["combined_id"] = inner["unique_id_x"].apply(lambda x: x.tolist()) + inner[
    "unique_id_y"
].apply(lambda x: x.tolist())

# %%
inner["combined_id"] = inner["combined_id"].apply(set).apply(list)


# %%
inner = inner.drop(["unique_id_x", "unique_id_y"], axis=1)

inner.rename(columns={"combined_id": "unique_id"}, inplace=True)


# %%
inner["combined_rights"] = inner["rights_x"].apply(lambda x: x.tolist()) + inner[
    "rights_y"
].apply(lambda x: x.tolist())

# %%
# remove duplicates
rights = inner["combined_rights"].to_list()

# %%
for j in range(len(rights)):
    print(j)
    rights[j] = [i for n, i in enumerate(rights[j]) if i not in rights[j][n + 1 :]]


# %%
rights = pd.Series(rights, name="rights")

inner["combined_rights"] = rights


# %%
inner = inner.drop(["rights_x", "rights_y"], axis=1)

inner.rename(columns={"combined_rights": "rights"}, inplace=True)

del rights, j

# %%
# keep columns with less missing values

kingdom_x_missing = inner["kingdom_x"].isna().sum()

kingdom_y_missing = inner["kingdom_y"].isna().sum()

# drop the kingdom column from inner that contains more missing values
if kingdom_x_missing > kingdom_y_missing:
    inner = inner.drop(["kingdom_x"], axis=1)
    inner.rename(columns={"kingdom_y": "kingdom"}, inplace=True)
else:
    inner = inner.drop(["kingdom_y"], axis=1)
    inner.rename(columns={"kingdom_x": "kingdom"}, inplace=True)

phylum_x_missing = inner["phylum_x"].isna().sum()

phylum_y_missing = inner["phylum_y"].isna().sum()

# drop the phylum column from inner that contains more missing values

if phylum_x_missing > phylum_y_missing:
    inner = inner.drop(["phylum_x"], axis=1)
    inner.rename(columns={"phylum_y": "phylum"}, inplace=True)
else:
    inner = inner.drop(["phylum_y"], axis=1)
    inner.rename(columns={"phylum_x": "phylum"}, inplace=True)

# find sum of missing values in inner['class_x']
class_x_missing = inner["class_x"].isna().sum()

class_y_missing = inner["class_y"].isna().sum()

# drop the class column from inner that contains more missing values
if class_x_missing > class_y_missing:
    inner = inner.drop(["class_x"], axis=1)
    inner.rename(columns={"class_y": "class"}, inplace=True)
else:
    inner = inner.drop(["class_y"], axis=1)
    inner.rename(columns={"class_x": "class"}, inplace=True)

# check
class_missing = inner["class"].isna().sum()

order_x_missing = inner["order_x"].isna().sum()

order_y_missing = inner["order_y"].isna().sum()

# drop the order column from inner that contains more missing values

if order_x_missing > order_y_missing:
    inner = inner.drop(["order_x"], axis=1)
    inner.rename(columns={"order_y": "order"}, inplace=True)
else:
    inner = inner.drop(["order_y"], axis=1)
    inner.rename(columns={"order_x": "order"}, inplace=True)

family_x_missing = inner["family_x"].isna().sum()

family_y_missing = inner["family_y"].isna().sum()

# drop the family column from inner that contains more missing values

if family_x_missing > family_y_missing:
    inner = inner.drop(["family_x"], axis=1)
    inner.rename(columns={"family_y": "family"}, inplace=True)
else:
    inner = inner.drop(["family_y"], axis=1)
    inner.rename(columns={"family_x": "family"}, inplace=True)


genus_x_missing = inner["genus_x"].isna().sum()

genus_y_missing = inner["genus_y"].isna().sum()

# drop the genus column from inner that contains more missing values

if genus_x_missing > genus_y_missing:
    inner = inner.drop(["genus_x"], axis=1)
    inner.rename(columns={"genus_y": "genus"}, inplace=True)
else:
    inner = inner.drop(["genus_y"], axis=1)
    inner.rename(columns={"genus_x": "genus"}, inplace=True)

# %%
del (
    genus_y_missing,
    genus_x_missing,
    family_y_missing,
    family_x_missing,
    order_y_missing,
    order_x_missing,
    class_y_missing,
    class_x_missing,
    phylum_y_missing,
    phylum_x_missing,
    kingdom_y_missing,
    kingdom_x_missing,
    class_missing,
)


# %%
# drop _merge column
inner = inner.drop("_merge", axis=1)

# %%
inner = inner[
    [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "scientific_name",
        "common_name",
        "species_type",
        "unique_id",
        "rights",
    ]
]


# %%
left = outer.loc[
    outer._merge == "left_only",
    [
        "kingdom_x",
        "phylum_x",
        "class_x",
        "order_x",
        "family_x",
        "genus_x",
        "scientific_name",
        "common_name",
        "species_type",
        "unique_id_x",
        "rights_x",
    ],
]


# %%
left.rename(
    columns={
        "kingdom_x": "kingdom",
        "phylum_x": "phylum",
        "class_x": "class",
        "order_x": "order",
        "family_x": "family",
        "genus_x": "genus",
        "unique_id_x": "unique_id",
        "rights_x": "rights",
    },
    inplace=True,
)


# %%
left = left[
    [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "scientific_name",
        "common_name",
        "species_type",
        "unique_id",
        "rights",
    ]
]

# %%
right = outer.loc[
    outer._merge == "right_only",
    [
        "kingdom_y",
        "phylum_y",
        "class_y",
        "order_y",
        "family_y",
        "genus_y",
        "scientific_name",
        "common_name",
        "species_type",
        "unique_id_y",
        "rights_y",
    ],
]


# %%
right.rename(
    columns={
        "kingdom_y": "kingdom",
        "phylum_y": "phylum",
        "class_y": "class",
        "order_y": "order",
        "family_y": "family",
        "genus_y": "genus",
        "unique_id_y": "unique_id",
        "rights_y": "rights",
    },
    inplace=True,
)


# %%
right = right[
    [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "scientific_name",
        "common_name",
        "species_type",
        "unique_id",
        "rights",
    ]
]


# %%
frames = [inner, left, right]

# concatenate dataframes
final = pd.concat(frames)

# reset index
final.reset_index(drop=True, inplace=True)


# %%
del frames, left, right, inner, outer


# %%
# read cleaned df into a parquet file where user can input the file path
df_merge_path = input("Enter the file path: ")


# write df to parquet file using pandas to_parquet
final.to_parquet(df_merge_path)
