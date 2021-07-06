from kafka import KafkaConsumer

consumer = KafkaConsumer("twitter",
  bootstrap_servers=['localhost:29092'],
  auto_offset_reset = "latest",
  enable_auto_commit=True,
  group_id="tech-core-easy-1")

for message in consumer:
  print(message)