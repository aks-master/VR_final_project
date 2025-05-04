import pandas as pd

# Path to your large CSV file
input_file = "valid_dataset.csv"  # Replace with your file path

# Read the CSV file into a DataFrame
print("Loading the dataset...")
df = pd.read_csv(input_file)

# Get the total number of rows
total_rows = len(df)
print(f"Total rows in the dataset: {total_rows}")

# Calculate the split point (e.g., split into two equal parts)
split_point = total_rows // 2

# Split the DataFrame into two parts
df_part1 = df.iloc[:split_point]  # First half
df_part2 = df.iloc[split_point:]  # Second half

# Save the two parts into new CSV files
output_file1 = "valid_dataset_part1.csv"  # Replace with your desired output path
output_file2 = "valid_dataset_part2.csv"  # Replace with your desired output path

print("Saving the first part...")
df_part1.to_csv(output_file1, index=False)
print("Saving the second part...")
df_part2.to_csv(output_file2, index=False)

print(f"Dataset split into two files: {output_file1} and {output_file2}")
print(f"Rows in part 1: {len(df_part1)}")
print(f"Rows in part 2: {len(df_part2)}")
