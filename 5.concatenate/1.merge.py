import pandas as pd

# Load the metadata CSV and select specific columns
metadata_df = pd.read_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\concatenate\valid_dataset_filtered_cleaned.csv"
)[["item_id", "main_image_id", "path"]]

# Add prefix 'images/small/' to the 'path' column
metadata_df["path"] = "images/small/" + metadata_df["path"].astype(str)

# Load the questions CSV
questions_df = pd.read_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\concatenate\vqa_dataset_combined_filtered_cleaned.csv"
)

# Merge the two DataFrames on 'item_id'
merged_df = pd.merge(metadata_df, questions_df, on="item_id", how="left")

# Save the merged DataFrame to a new CSV file
merged_df.to_csv("merged_dataset.csv", index=False)

print("Merged CSV saved as 'merged_dataset.csv'")
