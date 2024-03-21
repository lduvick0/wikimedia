from confluent_kafka import Producer, Consumer
import requests
import json
p=Producer({'bootstrap.servers': 'kafka:9092',
            'client.id': 'wikimedia_producer'})

topic='latest_events'

wikimedia_api_url='https://stream.wikimedia.org/v2/stream/recentchange'

c=Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'wikimedia_producer','auto.offset.reset': 'earliest'})
c.subscribe([topic])
#would split out but no
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), 'partition', msg.partition())


response = requests.get(wikimedia_api_url, stream=True)
for line in response.iter_lines():
    if line:
        # data=json.loads(line.decode('utf-8'))
        p.poll(0)
        p.produce(topic, value=line, callback=delivery_report)
        p.flush()
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error() is not None:
        print('error: '.format(msg.error()))
        continue
    print('Received message: {}'.format(msg.value().decode('utf-8')))