import hazelcast

client = hazelcast.HazelcastClient(
    cluster_members=[
        "localhost:5701",
        "localhost:5702",
        "localhost:5703",
    ]
)

# Простий тест — записати значення
my_map = client.get_map("test-map").blocking()
my_map.put("1", "Hello from Python!")
print("Read back:", my_map.get("1"))

client.shutdown()
