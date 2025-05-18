import pandas as pd
import os

# Windows paths
csv_file = r"C:\Users\Naval Kishore\Desktop\project vr\images\metadata\images.csv.gz"
image_dir = r"C:\Users\Naval Kishore\Desktop\project vr\images\small"
listings_file = r"C:\Users\Naval Kishore\Desktop\project vr\all_listings_metadata.csv"

# Ubuntu paths (uncomment if using Ubuntu)
# csv_file = "/home/user/project vr/images/metadata/images.csv.gz"
# image_dir = "/home/user/project vr/images/small"
# listings_file = "/home/user/project vr/all_listings_metadata.csv"

# Load image metadata
image_df = pd.read_csv(csv_file, compression="gzip")
image_df["full_path"] = image_df["path"].apply(lambda x: os.path.join(image_dir, x))
image_df["exists"] = image_df["full_path"].apply(os.path.exists)

# Load listings in chunks to manage memory
chunk_size = 50000
valid_dfs = []

for chunk in pd.read_csv(listings_file, chunksize=chunk_size):
    # Merge with images
    merged_df = chunk.merge(
        image_df, left_on="main_image_id", right_on="image_id", how="inner"
    )

    # Filter for English metadata and existing images
    valid_chunk = merged_df[
        (merged_df["exists"])
        & (merged_df["item_name"] != "")
        & (merged_df["language_tag"].str.startswith("en", na=False))
    ]
    if not valid_chunk.empty:
        valid_dfs.append(valid_chunk)

# Combine valid chunks
if valid_dfs:
    valid_df = pd.concat(valid_dfs, ignore_index=True)
else:
    valid_df = pd.DataFrame()

print("Valid products with images (English metadata):", len(valid_df))
print(
    "First 5 rows:\n",
    valid_df[["item_id", "item_name", "main_image_id", "full_path", "color"]].head(),
)

# Save linked data
valid_df.to_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\valid_dataset.csv", index=False
)
