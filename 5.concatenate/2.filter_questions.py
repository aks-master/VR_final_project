import pandas as pd

# Load the merged dataset
df = pd.read_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\concatenate\merged_dataset.csv"
)

# Group by main_image_id and count the number of questions
# question_counts = df.groupby('main_image_id').size().reset_index(name='question_count')
question_counts = df.groupby("item_id").size().reset_index(name="question_count")
# Filter to keep only main_image_id with exactly 10 questions
# valid_ids = question_counts[question_counts["question_count"] == 10]["main_image_id"]
valid_ids = question_counts[question_counts["question_count"] == 10]["item_id"]

# Filter the original dataframe to keep only rows with valid main_image_id
# filtered_df = df[df['main_image_id'].isin(valid_ids)]
filtered_df = df[df["item_id"].isin(valid_ids)]

# Save the filtered dataframe to a new CSV
filtered_df.to_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\concatenate\filtered_dataset_10_questions.csv",
    index=False,
)

print(f"Filtered dataset saved with {len(filtered_df)} rows.")
print(f"Number of unique main_image_id: {len(valid_ids)}")
