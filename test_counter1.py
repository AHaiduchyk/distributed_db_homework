import unittest
from task1 import (
    initialize_db,
    reset_counter,
    lost_update_worker,
    in_place_update_worker,
    row_level_locking_worker,
    optimistic_locking_worker,
    DB_CONFIG
)
import threading
import psycopg2
import time

def run_concurrent_workers(worker_func, threads_count=10):
    reset_counter()
    threads = []
    for _ in range(threads_count):
        t = threading.Thread(target=worker_func)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

def get_counter():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    value = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return value

class TestCounterUpdates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        initialize_db()

    def test_lost_update(self):
        run_concurrent_workers(lost_update_worker)
        final = get_counter()
        print(f"[Test] Lost Update: final counter = {final}")
        self.assertTrue(0 < final < 100000)

    def test_in_place_update(self):
        start = time.time()
        run_concurrent_workers(in_place_update_worker)
        end = time.time()
        final = get_counter()
        print(f"[Test] In-place Update: final counter = {final}, time = {end - start:.2f}s")
        self.assertEqual(final, 100000)

    def test_row_level_locking(self):
        start = time.time()
        run_concurrent_workers(row_level_locking_worker)
        end = time.time()
        final = get_counter()
        print(f"[Test] Row-level Locking: final counter = {final}, time = {end - start:.2f}s")
        self.assertEqual(final, 100000)

    def test_optimistic_locking(self):
        start = time.time()
        run_concurrent_workers(optimistic_locking_worker)
        end = time.time()
        final = get_counter()
        print(f"[Test] Optimistic Locking: final counter = {final}, time = {end - start:.2f}s")
        self.assertEqual(final, 100000)

if __name__ == "__main__":
    unittest.main()
