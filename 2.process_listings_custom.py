import json
import csv
import gzip
from glob import glob
import os

def has_english_value(data, field, lang_prefixes=["en", "en_us", "en_in", "en_ae", "en_gb", "en_au"]):
    values = data.get(field, [])
    if isinstance(values, list):
        for v in values:
            if isinstance(v, dict) and "language_tag" in v:
                for lang_prefix in lang_prefixes:
                    if v.get("language_tag", "").lower().startswith(lang_prefix.lower()):
                        return True
    return False

def extract_field(data, fields, lang_prefixes=["en", "en_us", "en_in", "en_ae", "en_gb", "en_au"], default="", field_name=""):
    for field in fields:
        values = data.get(field, [])
        # Case 1: Field value is a list of dictionaries
        if isinstance(values, list):
            for v in values:
                if isinstance(v, dict):
                    # For product_type, take the first value if language_tag is absent
                    if field_name == "product_type" and "language_tag" not in v and "value" in v:
                        return v.get("value", default), ""
                    # For other fields, filter by language_tag
                    if "language_tag" in v:
                        for lang_prefix in lang_prefixes:
                            if v.get("language_tag", "").lower().startswith(lang_prefix.lower()):
                                return v.get("value", default), v.get("language_tag", "")
        # Case 2: Field value is a single dictionary
        elif isinstance(values, dict):
            for lang_prefix in lang_prefixes:
                if values.get("language_tag", "").lower().startswith(lang_prefix.lower()):
                    return values.get("value", default), values.get("language_tag", "")
        # Case 3: Field value is a string
        elif isinstance(values, str):
            return values, ""
    return default, ""

def extract_bullet_points(bullets, lang_prefixes=["en", "en_us", "en_in", "en_ae", "en_gb", "en_au"]):
    if isinstance(bullets, list):
        return " | ".join(
            [
                b.get("value", "")
                for b in bullets
                if isinstance(b, dict)
                and any(
                    b.get("language_tag", "").lower().startswith(lang_prefix.lower())
                    for lang_prefix in lang_prefixes
                )
            ]
        )
    return ""

# Update this to your metadata folder path
input_dir = r"C:\Users\Naval Kishore\Desktop\project vr\listings\metadata"
json_gz_files = glob(os.path.join(input_dir, "listings_*.json.gz"))

output_rows = []
missing_fields_log = []
skipped_non_english = 0
missing_itemid_log = []
missing_itemid_count = 0

for file_path in json_gz_files:
    print(f"[+] Processing: {os.path.basename(file_path)}")
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            try:
                # Skip empty lines
                if not line.strip():
                    print(f"[!] Skipping line {line_number} in {file_path}: Empty line")
                    continue

                item = json.loads(line.strip())
                # Try different possible field names for item ID
                item_id = (
                    item.get("itemId", "")
                    or item.get("item_id", "")
                    or item.get("id", "")
                    or item.get("productId", "")
                    or item.get("asin", "")
                )
                if not item_id:
                    missing_itemid_count += 1
                    if missing_itemid_count <= 10 or missing_itemid_count % 100 == 0:
                        print(f"[!] Skipping line {line_number} in {file_path}: Missing itemId")
                    missing_itemid_log.append(
                        {
                            "file": os.path.basename(file_path),
                            "line": line_number,
                            "raw_line": line.strip(),
                        }
                    )
                    continue

                # Skip entries without English itemName
                if not has_english_value(item, "item_name"):
                    skipped_non_english += 1
                    missing_fields_log.append(
                        {
                            "item_id": item_id,
                            "file": os.path.basename(file_path),
                            "line": line_number,
                            "reason": "No English itemName",
                            "raw_data": item,
                        }
                    )
                    continue

                # Extract fields with fallback field names
                item_name, item_language_tag = extract_field(item, ["item_name"], field_name="item_name")
                color, _ = extract_field(item, ["color"], field_name="color")
                style, _ = extract_field(item, ["style"], field_name="style")
                product_type, _ = extract_field(item, ["product_type", "item_type", "type", "category"], field_name="product_type")
                brand, _ = extract_field(item, ["brand"], field_name="brand")

                row = {
                    "item_id": item_id,
                    "item_name": item_name,
                    "language_tag": item_language_tag,
                    "color": color,
                    "style": style,
                    "product_type": product_type,
                    "brand": brand,
                    "main_image_id": item.get("main_image_id", ""),
                    "bullet_points": extract_bullet_points(item.get("bullet_point", [])),
                }

                # Check for missing fields (for debugging)
                missing_fields = [
                    key for key, value in row.items()
                    if key not in ["item_id", "language_tag"] and value == ""
                ]
                if missing_fields:
                    # Include raw product_type data for debugging
                    raw_product_type = {
                        fname: item.get(fname, "Not found")
                        for fname in ["product_type", "item_type", "type", "category"]
                    }
                    missing_fields_log.append(
                        {
                            "item_id": item_id,
                            "file": os.path.basename(file_path),
                            "line": line_number,
                            "missing": missing_fields,
                            "raw_product_type": raw_product_type,
                            "raw_data": item,
                        }
                    )

                output_rows.append(row)

            except json.JSONDecodeError as e:
                print(f"[!] JSON decode error in {file_path}, line {line_number}: {e}")
                missing_itemid_log.append(
                    {
                        "file": os.path.basename(file_path),
                        "line": line_number,
                        "raw_line": line.strip(),
                        "error": str(e),
                    }
                )

# Save to single CSV in the project directory
if output_rows:
    output_file = r"C:\Users\Naval Kishore\Desktop\project vr\all_listings_metadata.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(output_rows[0].keys()))
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"[✅ Success] Exported {len(output_rows)} rows to {output_file}")
else:
    print("[⚠️ Warning] No valid metadata found.")

# Log missing fields, skipped entries, and missing itemId entries for debugging
if missing_fields_log or missing_itemid_log:
    log_file = r"C:\Users\Naval Kishore\Desktop\project vr\missing_fields_log.json"
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "missing_fields_or_non_english": missing_fields_log,
                "missing_itemid": missing_itemid_log,
            },
            f,
            indent=2,
        )
    print(f"[⚠️ Debug] Logged entries to {log_file}")
print(f"[ℹ️ Info] Skipped {skipped_non_english} entries with non-English itemName")
print(f"[ℹ️ Info] Skipped {missing_itemid_count} entries with missing itemId or JSON errors")