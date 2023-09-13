# %%
import pandas as pd
import numpy as np
import vaex

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_seq_items', 100)

# %%
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
# convert df_essential to pandas dataframe from vaex dataframe
df_essential = df_essential.to_pandas_df()

df_essential

# %%
# check if the species column contains any names that are longer than 2 words
# currently only keeping 2 word names and removing sub species


# change first value in species column of df_essential to 'a b c'
df_essential["species"][0] = "a b c"
df_essential["species"][3] = "d e f"

df_essential

# %%
# find number of words in the species column of df_essential
species_words = df_essential["species"].to_numpy()

# find number of words in each string in ndarray species_words
species_value = df_essential["species"].str.split().str.len()

species_values = np.array([len(word.split()) for word in species_words])

# %%
# if an item at index i in species_values is greater than 2, remove it from df_essential at the same index
# for i in range(len(species_values)):
#     if species_values[i] > 2:
#         df_essential = df_essential.drop(i)
for i in range(len(species_values)):
    if species_values[i] > 2:
        df_essential = df_essential[df_essential.row_index != i]

# # find items in species_value whose length is greater than 2
# species_values = species_value[species_value > 2]
# # convert species_value from expression to numpy array
# species_value = np.array([len(word.split()) for word in species_words])

# %%

#

# %%
df_essential.scientificName.unique().len()
df_essential.species.unique().len()


# %%
df_essential

# %%
# if there are any species names that are longer than 2 words, remove them from df_essential

# %%


# %%
# read cleaned df_essential into a parquet file where user can input the file path
df_clean_path = input("Enter the file path: ")
df_essential.export_parquet(df_clean_path)

# write df_essential to parquet file using pandas to_parquet
# df_essential.to_parquet(df_clean_path)
