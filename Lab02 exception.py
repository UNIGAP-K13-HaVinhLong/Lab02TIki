import json
import psycopg2
import glob

try:
    ## Ket noi DB PSQL
    conn = psycopg2.connect(
        host="localhost",
        database="lab01_tiki",
        user="postgres",
        password="halong123"
    )
    cur = conn.cursor()

    ## Tao table va cac cot
    cur.execute("""
    CREATE TABLE IF NOT EXISTS TIKI_PRODUCTS(
        id INTEGER PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price INTEGER,
        description TEXT,
        image_url TEXT
        )
    """)
    for file_path in glob.glob('/home/long/UNIGAP/Lab01_File/*.json'):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            for product in data:
                try:
                    cur.execute("""
                    INSERT INTO TIKI_PRODUCTS (id, name, url_key, price, description, image_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """, (
                        product['id'],
                        product['name'],
                        product['url_key'],
                        product['price'],
                        product['description'],
                        json.dumps(product['image_url'])
                    ))
                except psycopg2.Error as e:
                    print(f'Loi khi them san pham {product['id']} vao DB: {e}')
            print(f"Da import thanh cong  {file_path}")
        except json.JSONDecodeError as e:
            print(f'Loi khong doc duoc file {file_path}: {e}')
    conn.commit()
except psycopg2.Error as e:
    print(f'Loi khong ket noi duoc voi DB: {e}')
except Exception as e:
    print(f'Loi khac, chua xac dinh: {e}')
finally:
    if conn:
        cur.close()
        conn.close()