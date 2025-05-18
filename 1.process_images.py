import pandas as pd
import os
from PIL import Image

# Windows paths
csv_file = r"C:\Users\Naval Kishore\Desktop\project vr\images\metadata\images.csv.gz"
image_dir = r"C:\Users\Naval Kishore\Desktop\project vr\images\small"


# Load image metadata
image_df = pd.read_csv(csv_file, compression="gzip")

# Add full image paths
image_df["full_path"] = image_df["path"].apply(lambda x: os.path.join(image_dir, x))

# Check if images exist
image_df["exists"] = image_df["full_path"].apply(os.path.exists)
print("Total images:", len(image_df))
print("Images found:", image_df["exists"].sum())
print("First 5 rows:\n", image_df[["image_id", "full_path", "exists"]].head())

# Display one image
sample_image = image_df[image_df["exists"]]["full_path"].iloc[0]
img = Image.open(sample_image)
img.show()
print("Sample image path:", sample_image)
image_df.to_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\processed_images.csv", index=False
)
