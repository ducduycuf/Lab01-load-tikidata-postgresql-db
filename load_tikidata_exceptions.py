import os
import json
import psycopg2
from psycopg2.extras import execute_values
import re

# db connection
try:
    conn = psycopg2.connect(
        dbname="suppliers",
        user="postgres",
        password="123456",
        port="5432"
    )
    cur = conn.cursor()
    print("Database connected successfully!")
except psycopg2.OperationalError as e:
    print(f"Cannot connect to database: {e}")
    exit(1)
except Exception as e:
    print(f"Unexpected error connecting to database: {e}")
    exit(1)


# loop through files
data_folder = "/home/duy/Documents/Task16-Lab01/200 batch"

for filename in sorted(os.listdir(data_folder)):
    if not filename.endswith(".json"):
        continue
    
    match = re.search(r'batch_(\d+)', filename)
    batch_no = int(match.group(1)) if match else 0
    file_path = os.path.join(data_folder, filename)
    
    try:
        with open(file_path, "r") as f:
            products = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filename}")
        failed_batches += 1
        continue
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in {filename}: {e}")
        failed_batches += 1
        continue
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        failed_batches += 1
        continue

#insert data with error handling
    values = [
        (
            product["id"],
            product.get("name"),
            product.get("url_key"),
            product.get("price"),
            product.get("description"),
            json.dumps(product.get("images", [])),
            batch_no
        )
        for product in products
    ]
    
    try:
        execute_values(
            cur,
            """
            INSERT INTO tiki_products
            (product_id, name, url_key, price, description, images, batch_no)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING;
            """,
            values
        )
        conn.commit()
        successful_batches += 1
        print(f"Batch {batch_no} inserted successfully ({len(products)} records).")
    
    except psycopg2.IntegrityError as e:
        conn.rollback()
        failed_batches += 1
        print(f"Integrity error in batch {batch_no}: {e}")
    except psycopg2.DataError as e:
        conn.rollback()
        failed_batches += 1
        print(f"Data error in batch {batch_no}: {e}")
    except Exception as e:
        conn.rollback()
        failed_batches += 1
        print(f"Failed to insert batch {batch_no}: {e}")

cur.close()
conn.close()

print("All batches loaded successfully.")