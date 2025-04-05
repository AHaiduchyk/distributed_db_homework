import hazelcast
import threading
import time
import os
from datetime import datetime

def log(thread_id, message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_message = f"[{timestamp}] [Thread-{thread_id}] {message}"
    print(log_message)
    log_path = f"logs/thread-{thread_id}.log"
    with open(log_path, "a") as f:
        f.write(log_message + "\n")


def increment_with_lock(my_map, key, thread_id, total_iterations=10_000):
    log(thread_id, "Started")
    for i in range(total_iterations):
        my_map.lock(key)
        try:
            value = my_map.get(key)
            my_map.put(key, value + 1)
        finally:
            my_map.unlock(key)

        if i % 1000 == 0 or i == total_iterations - 1:
            log(thread_id, f"Iteration {i}")
    log(thread_id, "Finished")


if __name__ == "__main__":
    if not os.path.exists("logs"):
        os.makedirs("logs")

    print("Connecting to Hazelcast cluster...")
    client = hazelcast.HazelcastClient(
        cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
    )
    my_map = client.get_map("counter-map-lock").blocking()
    counter_key = "counter"
    my_map.put(counter_key, 0)
    print("Connected. Counter reset to 0.\n")

    threads = []
    start_time = time.time()

    print("Starting 10 threads with lock...")
    for i in range(10):
        t = threading.Thread(
            target=increment_with_lock,
            args=(my_map, counter_key, i)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    final_value = my_map.get(counter_key)

    print("\nAll threads finished.")
    print(f"Final counter value (lock): {final_value}")
    print(f"Expected: 100000")
    print(f"Missing: {100000 - final_value}")
    print(f"Time taken: {end_time - start_time:.2f} seconds")

    client.shutdown()
    print("Hazelcast client shut down.")
