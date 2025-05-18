import pandas as pd
import os
import shutil
from tqdm import tqdm

# Paths
filtered_csv_path = r'C:\Users\Naval Kishore\Desktop\project vr\concatenate\filtered_dataset_10_questions.csv'
original_images_dir = r'C:\Users\Naval Kishore\Desktop\project vr'  # Replace with the path to your original ABO dataset
filtered_images_dir = r'C:\Users\Naval Kishore\Desktop\project vr\filtered_images_corrected'  # New directory for corrected filtered images

# Load the filtered dataset
df = pd.read_csv(filtered_csv_path)

# Get unique main_image_id and path pairs (take the first occurrence for each main_image_id)
unique_images = df[['main_image_id', 'path']].drop_duplicates(subset='main_image_id')

# Verify the number of unique images
print(f"Number of unique images to copy: {len(unique_images)}")

# Create the filtered directory if it doesn't exist
if not os.path.exists(filtered_images_dir):
    os.makedirs(filtered_images_dir)

# Copy images to the new directory while preserving the structure
for _, row in tqdm(unique_images.iterrows(), total=len(unique_images), desc="Copying images"):
    img_path = row['path']
    
    # Source path (original dataset)
    src_path = os.path.join(original_images_dir, img_path)
    
    # Destination path (filtered dataset)
    dst_path = os.path.join(filtered_images_dir, img_path)
    
    # Create the directory structure in the filtered folder if it doesn't exist
    dst_dir = os.path.dirname(dst_path)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    
    # Copy the image if it exists in the original dataset
    if os.path.exists(src_path):
        shutil.copy2(src_path, dst_path)
    else:
        print(f"Warning: Image not found at {src_path}")

print("Image copying completed!")