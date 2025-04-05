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


def increment_no_lock(my_map, key, thread_id, total_iterations=10_000):
    log(thread_id, f"Started with {total_iterations} iterations")
    for i in range(total_iterations):
        
        current = my_map.get(key)
        # print(current)
        my_map.put(key, current + 1)

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
    my_map = client.get_map("counter-map").blocking()
    my_map.put("counter", 0)
    print("Connected. Counter reset to 0.\n")

    threads = []
    total_iterations = 10_000
    start_time = time.time()

    print(f"Starting 10 threads with {total_iterations} iterations each...\n")
    for i in range(10):
        t = threading.Thread(target=increment_no_lock, args=(my_map, "counter", i, total_iterations))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    final_value = my_map.get("counter")

    print("\nAll threads finished.")
    print(f"Final counter value (no lock): {final_value}")
    print(f"Time taken: {end_time - start_time:.2f} seconds")

    client.shutdown()
    print("Hazelcast client shut down.")
