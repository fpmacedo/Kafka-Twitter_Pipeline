from kafka import KafkaConsumer

consumer = KafkaConsumer("t1",
  bootstrap_servers=['0.0.0.0:29092'],
  auto_offset_reset = "latest",
  enable_auto_commit=True,
  group_id="tech-core-easy-1")

for message in consumer:
  print(message)