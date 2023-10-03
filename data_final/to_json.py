# %%
import pandas as pd
import numpy as np
import json

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
# df_path = input("Enter the file path: ")
# df.to_csv(df_path, chunksize=1000000, index=False)

# file too big for json
# df.to_json(df_path, orient="records", force_ascii=False)


# %%
holder_dictionary = df.to_dict(orient="records")

# %%


df_path = input("Enter the file path: ")


with open(df_path, "w") as outfile:
    json.dump(holder_dictionary, outfile)
