import os
import json
import psycopg2
from psycopg2.extras import execute_values
import re

# --- PostgreSQL connection ---
conn = psycopg2.connect(
    dbname="tiki",
    user="postgres",
    password="123456",
    port="5432"
)
cur = conn.cursor() #tell psycopg2 where to execute queries

# --- Folder containing JSON batch files ---
data_folder = "/home/duy/Documents/Task16-Lab01/200 batch"

# --- Loop through all JSON files ---
for filename in sorted(os.listdir(data_folder)):
    if not filename.endswith(".json"):
        continue

    match = re.search(r'batch_(\d+)', filename)         #extract batch_number from filename
    batch_no = int(match.group(1)) if match else 0      #extract the number

    file_path = os.path.join(data_folder, filename)

    with open(file_path, "r") as f:
        products = json.load(f)

    # Prepare values for insertion
    values = [
        (
            product["id"],
            product.get("name"),
            product.get("url_key"),
            product.get("price"),
            product.get("description"),
            json.dumps(product.get("images", [])), #because images is a list --> needs to store as JSON string so SQL can handle it
            batch_no
        )
        for product in products
    ]

    # Bulk insert - instead of inserting one by one --> execute many at once
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
    print(f"Batch {batch_no} inserted successfully ({len(products)} records).")

cur.close()
conn.close()
print("All batches loaded successfully.")