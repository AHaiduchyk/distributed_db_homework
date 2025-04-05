import psycopg2
import threading
import time
from datetime import datetime

DB_CONFIG = {
    "dbname": "mydatabase",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

log_file_path = "/Users/deslang/Documents/distributed_db_homework/distributed_db_homework/task1_pg_counter.log"

def log_to_file(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")


def initialize_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_counter (
            user_id INT PRIMARY KEY,
            counter INT NOT NULL,
            version INT NOT NULL
        )
    """)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM user_counter WHERE user_id = 1")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)")
        conn.commit()
    cursor.close()
    conn.close()


def reset_counter():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1")
    conn.commit()
    cursor.close()
    conn.close()


def lost_update_worker():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    for _ in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0] + 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
        conn.commit()
    cursor.close()
    conn.close()


def in_place_update_worker():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    for _ in range(10000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()
    cursor.close()
    conn.close()


def row_level_locking_worker():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    for _ in range(10000):
        cursor.execute("BEGIN")
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0] + 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
        conn.commit()
    cursor.close()
    conn.close()


def optimistic_locking_worker():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    for _ in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            counter, version = cursor.fetchone()
            cursor.execute("""
                UPDATE user_counter
                SET counter = %s, version = %s
                WHERE user_id = %s AND version = %s
            """, (counter + 1, version + 1, 1, version))
            conn.commit()
            if cursor.rowcount > 0:
                break
    cursor.close()
    conn.close()


def updated_run_test(worker_func, label):
    reset_counter()
    threads = []
    start = time.time()
    log_to_file(f"=== {label} ===")
    for i in range(10):
        t = threading.Thread(target=worker_func)
        t.start()
        threads.append(t)
        log_to_file(f"{label} - Thread {i+1} started")
    for i, t in enumerate(threads):
        t.join()
        log_to_file(f"{label} - Thread {i+1} joined")
    end = time.time()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    expected = "100000" if label != "Lost Update" else "<100000 (expected loss)"
    duration = f"{end - start:.2f}"
    result_line = f"{label} completed in {duration} seconds. Final counter: {result} (Expected: {expected})"
    print(result_line)
    log_to_file(result_line)


# Запускаємо всі тести з логуванням
initialize_db()
log_to_file("=== PostgreSQL Counter Update Test Started ===")
updated_run_test(lost_update_worker, "Lost Update")
updated_run_test(in_place_update_worker, "In-place Update")
updated_run_test(row_level_locking_worker, "Row-level Locking")
updated_run_test(optimistic_locking_worker, "Optimistic Locking")
log_to_file("=== PostgreSQL Counter Update Test Completed ===")

log_file_path
