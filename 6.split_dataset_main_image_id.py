import pandas as pd
from sklearn.model_selection import train_test_split
import os

# Paths
csv_path = r"C:\Users\Naval Kishore\Desktop\dataset_vr\main_image_id\filtered_dataset_10_questions.csv"
output_dir = r"C:\Users\Naval Kishore\Desktop\dataset_vr\main_image_id"

# Load dataset
df = pd.read_csv(csv_path)
print(
    f"Total: {len(df)} rows, {df['main_image_id'].nunique()} unique main_image_ids, {df['item_id'].nunique()} unique item_ids"
)

# Group by main_image_id to ensure all 10 questions stay together
unique_images = df[["main_image_id"]].drop_duplicates()
train_images, temp_images = train_test_split(
    unique_images, test_size=0.2, random_state=42
)
val_images, test_images = train_test_split(temp_images, test_size=0.5, random_state=42)

# Merge back to get full rows
train_df = df[df["main_image_id"].isin(train_images["main_image_id"])]
val_df = df[df["main_image_id"].isin(val_images["main_image_id"])]
test_df = df[df["main_image_id"].isin(test_images["main_image_id"])]

# Save splits
train_df.to_csv(os.path.join(output_dir, "train_main_image_id.csv"), index=False)
val_df.to_csv(os.path.join(output_dir, "val_main_image_id.csv"), index=False)
test_df.to_csv(os.path.join(output_dir, "test_main_image_id.csv"), index=False)

# Print stats
print(f"Train: {len(train_df)} rows, {train_df['main_image_id'].nunique()} images")
print(f"Validation: {len(val_df)} rows, {val_df['main_image_id'].nunique()} images")
print(f"Test: {len(test_df)} rows, {test_df['main_image_id'].nunique()} images")
