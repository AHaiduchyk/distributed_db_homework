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

def increment_atomic(counter, thread_id, total_iterations=10_000):
    log(thread_id, "Started")
    for i in range(total_iterations):
        counter.increment_and_get()
        if i % 1000 == 0 or i == total_iterations - 1:
            log(thread_id, f"Iteration {i}")
    log(thread_id, "Finished")

if __name__ == "__main__":
    if not os.path.exists("logs"):
        os.makedirs("logs")

    print("Connecting to Hazelcast cluster with CP Subsystem...")
    client = hazelcast.HazelcastClient(
        cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
    )

    # Виклик через CP Subsystem (правильно!)
    counter = client.cp_subsystem.get_atomic_long("counter-atomic").blocking()
    counter.set(0)
    print("Connected. IAtomicLong counter reset to 0.\n")

    threads = []
    start_time = time.time()

    print("Starting 10 threads using IAtomicLong...\n")
    for i in range(10):
        t = threading.Thread(target=increment_atomic, args=(counter, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    final_value = counter.get()

    print("\nAll threads finished.")
    print(f"Final counter value (atomic): {final_value}")
    print(f"Expected: 100000")
    print(f"Missing: {100000 - final_value}")
    print(f"Time taken: {end_time - start_time:.2f} seconds")

    counter.destroy()
    client.shutdown()
    print("Hazelcast client shut down and drop.")
