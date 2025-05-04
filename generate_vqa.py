import pandas as pd
import requests
import base64
import json
import re
import os
import time
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed

# Windows path
valid_df = pd.read_csv(r"C:\Users\Naval Kishore\Desktop\project vr\valid_dataset.csv")

# Ubuntu path (uncomment if using Ubuntu)
# valid_df = pd.read_csv(r"/home/user/project vr/valid_dataset.csv")

# Load existing vqa_dataset.csv to skip processed items
output_path = r"C:\Users\Naval Kishore\Desktop\project vr\vqa_dataset.csv"
# Ubuntu output path (uncomment if using Ubuntu)
# output_path = r"/home/user/project vr/vqa_dataset.csv"

processed_ids = set()
if os.path.exists(output_path):
    existing_df = pd.read_csv(output_path)
    processed_ids = set(existing_df["Image_ID"].unique())

subset_df = valid_df[valid_df["item_id"].isin(processed_ids) == False].head(10)

# Multiple API keys for rotation
api_keys = [
    "AIzaSyALeRCqHiNyV5-Y3ibo9fwPk4v65Flbu_c",
    "AIzaSyAaPKXi9jdYa2kXsh4YADq5RwRWnZ5erPE",
]

# Rate limiting
RATE_LIMIT_RPM = 15
RATE_LIMIT_DELAY = 60 / RATE_LIMIT_RPM
DAILY_LIMIT = 5

api_key_usage = {
    key: {
        "count": 0,
        "first_request_time": None,
        "request_timestamps": [],
    }
    for key in api_keys
}


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


def generate_questions(image_path, metadata, api_key):
    try:
        with open(image_path, "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode("utf-8")
        prompt = (
            f"Given this product image (base64) and metadata {metadata}, "
            "generate exactly 10 unique visual questions with single-word answers about the product’s appearance, its description and inormation in bullet_points and product_type columns also. "
            "Include the following questions: 'What is the color?' and 'What is the texture? and 'what is the product name ''what is the product type?'.'what is the shape of the product ?' "
            "For the other question, choose a diverse visual aspect (e.g., shape, material, pattern, style, brand visibility, or suitability for its product type). "
            "Examples of the third question: 'What is the shape?', 'What is the material?', 'Is the brand logo visible?', 'Does the design suit its product type?'. "
            "Avoid repetitive questions and vary the phrasing. "
            "Format: JSON array of objects with 'question' and 'answer'."
        )
        response = requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent",
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
        response_text = response.text
        print(
            f"Raw API response for {image_path} with API key {api_key[-5:]}:\n{response_text}\n"
        )

        data = response.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        json_match = re.search(r"```json\n([\s\S]*?)\n```", text, re.DOTALL)
        if json_match:
            json_text = json_match.group(1).strip()
        else:
            print(f"No JSON block found in response for {image_path}")
            return []

        try:
            questions = json.loads(json_text)
            current_time = time.time()
            api_key_usage[api_key]["count"] += 1
            api_key_usage[api_key]["request_timestamps"].append(current_time)
            print(
                f"API key {api_key[-5:]} usage: {api_key_usage[api_key]['count']}/{DAILY_LIMIT}"
            )
            return questions
        except json.JSONDecodeError as e:
            print(f"JSON parse error for {image_path}: {e}\nJSON text: {json_text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"API request error for {image_path}: {e}")
        return []
    except KeyError as e:
        print(f"Unexpected response structure for {image_path}: {e}")
        return []
    except Exception as e:
        print(f"General error for {image_path}: {e}")
        return []


api_key_iterator = cycle(api_keys)
questions = []
batch_size = len(api_keys)

for i in range(0, len(subset_df), batch_size):
    batch = subset_df.iloc[i : i + batch_size]
    batch_questions = []

    tasks = []
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        for _, row in batch.iterrows():
            image_path = row["full_path"]
            item_id = row["item_id"]
            if item_id in processed_ids:
                print(f"Skipping already processed item_id: {item_id}")
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

            all_exhausted = False  # Ensure variable is defined
            while True:
                api_key = next(api_key_iterator)
                can_proceed, reason = check_limits(api_key)
                if can_proceed:
                    break
                print(f"Skipping API key {api_key[-5:]}: {reason}")
                all_exhausted = all(
                    api_key_usage[key]["count"] >= DAILY_LIMIT for key in api_keys
                )
                if all_exhausted:
                    print(
                        "All API keys have reached their daily limit. Stopping processing."
                    )
                    break
            if all_exhausted:
                break

            print(f"Using API key: {api_key[-5:]} for item_id: {item_id}")
            future = executor.submit(generate_questions, image_path, metadata, api_key)
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

    if i + batch_size < len(subset_df):
        print(f"Waiting {RATE_LIMIT_DELAY} seconds for rate limit...")
        time.sleep(RATE_LIMIT_DELAY)

if questions:
    qa_df = pd.DataFrame(questions)
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        qa_df = pd.concat([existing_df, qa_df], ignore_index=True)
    qa_df.to_csv(output_path, index=False)
    print("Generated questions:", len(qa_df))
    print("First 5 questions:\n", qa_df.head())
else:
    print("No new questions generated.")
    if os.path.exists(output_path):
        qa_df = pd.read_csv(output_path)
        print("Existing questions:", len(qa_df))
        print("First 5 existing questions:\n", qa_df.head())
    else:
        print("No existing vqa_dataset.csv found.")
