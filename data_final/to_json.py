# %%
import pandas as pd

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_seq_items', 100)

# 400 meter coordinate accuracy
# 1923 earliest year


# %%
# read in cleaned andmerged data
df_path = input("Enter the file path: ")

df = pd.read_parquet(df_path)


# %%

for i in range(0, len(df), 100000):
    df_new = df.iloc[i : i + 100000]
    df_path = f"/media/muskrat/T7 Shield/eco_data/v2/data_final/species_v2.0_json/species_v2.0_{i}.json"
    df_new.to_json(df_path, orient="records", force_ascii=False)

# %%
# holder_dictionary = df.to_dict(orient="records")

# %%


# df_path = input("Enter the file path: ")


# with open(df_path, "w") as outfile:
#     json.dump(holder_dictionary, outfile)
