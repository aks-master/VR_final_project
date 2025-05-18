import pandas as pd
import requests
import base64
import json
import re
import os
import time
import logging
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(
    filename=r"C:\Users\Naval Kishore\Desktop\project vr\vqa_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load dataset
valid_df = pd.read_csv(
    r"C:\Users\Naval Kishore\Desktop\project vr\valid_dataset_part2.csv"
)
output_path = r"C:\Users\Naval Kishore\Desktop\project vr\vqa_dataset_part2.csv"
failed_items_path = r"C:\Users\Naval Kishore\Desktop\project vr\failed_items.csv"

# Load processed IDs
processed_ids = set()
if os.path.exists(output_path):
    existing_df = pd.read_csv(output_path)
    processed_ids = set(existing_df["Image_ID"].unique())
logging.info(f"Already processed item IDs: {len(processed_ids)}")
print(f"Already processed item IDs: {len(processed_ids)}")

# Select all unprocessed items
subset_df = valid_df[~valid_df["item_id"].isin(processed_ids)]
logging.info(f"Total unprocessed items: {len(subset_df)}")
print(f"Total unprocessed items: {len(subset_df)}")

# API keys


api_keys = [
    "AIzaSyAjzG0glNen-U6zAtVqn-GrDHHM94UA60I",
]


# Rate limiting
RATE_LIMIT_RPM = 30  # Requests per minute
RATE_LIMIT_DELAY = 60 / RATE_LIMIT_RPM
DAILY_LIMIT = 300  # Adjusted to match previous logs
RETRY_ATTEMPTS = 3
RETRY_DELAY = 10  # Seconds to wait before retrying after 429
SAVE_INTERVAL = 100  # Save after every 10 API calls

api_key_usage = {
    key: {"count": 0, "first_request_time": None, "request_timestamps": []}
    for key in api_keys
}

# Track total API calls
total_api_calls = 0


def validate_api_key(api_key):
    """Validate API key by making a test request."""
    try:
        response = requests.post(
            "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": "Test"}]}]},
            params={"key": api_key},
        )
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        logging.error(f"API key {api_key[-5:]} validation failed: {e}")
        print(f"API key {api_key[-5:]} validation failed: {e}")
        return False


# Validate API keys
valid_api_keys = [key for key in api_keys if validate_api_key(key)]
if not valid_api_keys:
    logging.error("No valid API keys. Exiting.")
    print("No valid API keys. Exiting.")
    exit(1)
logging.info(f"Valid API keys: {len(valid_api_keys)}")
print(f"Valid API keys: {len(valid_api_keys)}")


def check_limits(api_key):
    current_time = time.time()
    usage = api_key_usage[api_key]
    if usage["first_request_time"] is None:
        usage["first_request_time"] = current_time
    if current_time - usage["first_request_time"] >= 86400:
        usage["count"] = 0
        usage["first_request_time"] = current_time
        usage["request_timestamps"] = []
    if usage["count"] >= DAILY_LIMIT:
        return False, "Daily limit reached"
    usage["request_timestamps"] = [
        t for t in usage["request_timestamps"] if current_time - t < 60
    ]
    if len(usage["request_timestamps"]) >= RATE_LIMIT_RPM:
        return False, "Rate limit reached"
    return True, "OK"


def generate_questions(image_path, metadata, api_key, item_id):
    global total_api_calls
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with open(image_path, "rb") as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode("utf-8")
            prompt = (
                f"Given this product image (base64) and metadata {metadata}, "
                "generate exactly 10 unique visual questions with single-word answers about the product’s appearance, its description and information in bullet_points and product_type columns also. "
                "Include the following questions: 'What is the color?' and 'What is the texture?' and 'What is the product name?' and 'What is the product type?' and 'What is the shape of the product?' "
                "For the other questions, choose diverse visual aspects (e.g., material, pattern, style, brand visibility, or suitability for its product type). "
                "Examples: 'What is the material?', 'Is the brand logo visible?', 'Does the design suit its product type?'. "
                "Avoid repetitive questions and vary the phrasing. "
                "Format: JSON array of objects with 'question' and 'answer'."
            )
            response = requests.post(
                "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [
                        {
                            "parts": [
                                {"text": prompt},
                                {
                                    "inline_data": {
                                        "mime_type": "image/jpeg",
                                        "data": img_base64,
                                    }
                                },
                            ]
                        }
                    ]
                },
                params={"key": api_key},
            )
            response.raise_for_status()
            data = response.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            json_match = re.search(r"```json\n([\s\S]*?)\n```", text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1).strip()
                questions = json.loads(json_text)
                current_time = time.time()
                api_key_usage[api_key]["count"] += 1
                api_key_usage[api_key]["request_timestamps"].append(current_time)
                total_api_calls += 1
                logging.info(
                    f"API key {api_key[-5:]} usage: {api_key_usage[api_key]['count']}/{DAILY_LIMIT} for item_id: {item_id}"
                )
                print(
                    f"API key {api_key[-5:]} usage: {api_key_usage[api_key]['count']}/{DAILY_LIMIT}"
                )
                return questions
            else:
                logging.error(
                    f"No JSON block found in response for {image_path} (item_id: {item_id})"
                )
                print(f"No JSON block found in response for {image_path}")
                return []
        except requests.exceptions.HTTPError as e:
            logging.error(
                f"HTTP error for {image_path} (item_id: {item_id}, attempt {attempt + 1}/{RETRY_ATTEMPTS}): {e}"
            )
            print(f"HTTP error for {image_path}: {e}")
            if e.response.status_code == 429:
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(RETRY_DELAY)
                    continue
            return []
        except Exception as e:
            logging.error(f"Error for {image_path} (item_id: {item_id}): {e}")
            print(f"Error for {image_path}: {e}")
            return []
    logging.error(
        f"Failed to process {image_path} (item_id: {item_id}) after {RETRY_ATTEMPTS} attempts"
    )
    print(f"Failed to process {image_path} after {RETRY_ATTEMPTS} attempts")
    # Log failed item
    failed_item = pd.DataFrame(
        [
            {
                "item_id": item_id,
                "image_path": image_path,
                "error": "Failed after retries",
            }
        ]
    )
    if os.path.exists(failed_items_path):
        existing_failed = pd.read_csv(failed_items_path)
        failed_item = pd.concat([existing_failed, failed_item], ignore_index=True)
    failed_item.to_csv(failed_items_path, index=False)
    return []


def save_questions(questions, output_path):
    """Save questions to CSV with error handling."""
    if not questions:
        return
    qa_df = pd.DataFrame(questions)
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        qa_df = pd.concat([existing_df, qa_df], ignore_index=True)
    try:
        qa_df.to_csv(output_path, index=False)
        logging.info(f"Saved {len(qa_df)} questions to {output_path}")
        print(f"Saved {len(qa_df)} questions to {output_path}")
    except PermissionError:
        backup_path = output_path.replace(".csv", f"_backup_{int(time.time())}.csv")
        logging.warning(f"Cannot write to {output_path}. Saving to {backup_path}")
        print(f"Cannot write to {output_path}. Saving to {backup_path}")
        qa_df.to_csv(backup_path, index=False)


api_key_iterator = cycle(valid_api_keys)
questions = []
batch_size = len(valid_api_keys)  # Use all valid API keys
processed_count = 0

try:
    for i in range(0, len(subset_df), batch_size):
        print(
            f"Processing batch {i//batch_size + 1}: items {i} to {min(i + batch_size, len(subset_df)) - 1}"
        )
        batch = subset_df.iloc[i : i + batch_size]
        batch_questions = []

        # Get available keys and limit batch size to available keys
        available_keys = [key for key in valid_api_keys if check_limits(key)[0]]
        if not available_keys:
            print("All API keys have reached their daily limit. Stopping processing.")
            logging.warning(
                "All API keys have reached their daily limit. Stopping processing."
            )
            break
        current_batch_size = min(len(available_keys), len(batch))
        if current_batch_size == 0:
            print("No available API keys for this batch. Stopping processing.")
            logging.warning(
                "No available API keys for this batch. Stopping processing."
            )
            break

        tasks = []
        with ThreadPoolExecutor(max_workers=current_batch_size) as executor:
            key_cycle = cycle(available_keys)
            for j, row in enumerate(batch.iterrows()):
                if j >= current_batch_size:
                    break  # Limit to available keys
                _, row = row
                image_path = row["full_path"]
                item_id = row["item_id"]
                if item_id in processed_ids:
                    print(f"Skipping already processed item_id: {item_id}")
                    logging.info(f"Skipping already processed item_id: {item_id}")
                    continue
                metadata = {
                    "color": row["color"] if pd.notna(row["color"]) else "",
                    "item_name": row["item_name"] if pd.notna(row["item_name"]) else "",
                    "bullet_points": (
                        row["bullet_points"] if pd.notna(row["bullet_points"]) else ""
                    ),
                    "style": row["style"] if pd.notna(row["style"]) else "",
                    "product_type": (
                        row["product_type"] if pd.notna(row["product_type"]) else ""
                    ),
                    "brand": row["brand"] if pd.notna(row["brand"]) else "",
                }
                api_key = next(key_cycle)
                print(f"Using API key: {api_key[-5:]} for item_id: {item_id}")
                logging.info(f"Using API key: {api_key[-5:]} for item_id: {item_id}")
                future = executor.submit(
                    generate_questions, image_path, metadata, api_key, item_id
                )
                tasks.append((future, item_id))
            for future, item_id in tasks:
                qas = future.result()
                for qa in qas:
                    batch_questions.append(
                        {
                            "Image_ID": item_id,
                            "Question": qa["question"],
                            "Correct_Answer": qa["answer"],
                        }
                    )
        questions.extend(batch_questions)
        processed_count += (
            len(batch_questions) // 10
        )  # Each item generates 10 questions
        print(f"Processed {processed_count}/{len(subset_df)} items")
        logging.info(f"Processed {processed_count}/{len(subset_df)} items")

        # Save questions if total_api_calls is a multiple of SAVE_INTERVAL
        if total_api_calls > 0 and total_api_calls % SAVE_INTERVAL == 0:
            print(f"Reached {total_api_calls} API calls. Saving intermediate results.")
            logging.info(
                f"Reached {total_api_calls} API calls. Saving intermediate results."
            )
            save_questions(questions, output_path)
            questions = []  # Clear questions to avoid duplication

        if i + batch_size >= len(subset_df):
            print("All items processed.")
            logging.info("All items processed.")
            break
        if not available_keys:
            print("Stopping: All API keys are exhausted.")
            logging.warning("Stopping: All API keys are exhausted.")
            break
        print(f"Waiting {RATE_LIMIT_DELAY} seconds for rate limit...")
        logging.info(f"Waiting {RATE_LIMIT_DELAY} seconds for rate limit...")
        time.sleep(RATE_LIMIT_DELAY)

except KeyboardInterrupt:
    print("Script interrupted. Saving partial results...")
    logging.warning("Script interrupted. Saving partial results...")
    if questions:
        save_questions(questions, output_path)
    print("Exiting.")
    logging.info("Exiting.")
    exit(0)

# Save any remaining questions
if questions:
    save_questions(questions, output_path)
    questions = []  # Clear questions
else:
    print("No new questions generated.")
    logging.info("No new questions generated.")

# Print final results
if os.path.exists(output_path):
    qa_df = pd.read_csv(output_path)
    print("Total questions in dataset:", len(qa_df))
    print("Last 5 questions:\n", qa_df.tail())
    logging.info(f"Total questions in dataset: {len(qa_df)}")
else:
    print("No vqa_dataset.csv found.")
    logging.info("No vqa_dataset.csv found.")
