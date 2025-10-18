# Easy Pika
## info
Easy Pika is a simple library that allows you to create a Pika connection and channel with single code based on Pika.
Easy Pike unify exchange and queue declare, summary the most common usage and help user to easy call.
## install
```bash
uv pip install ./easypika-0.7.0-py3-none-any.whl
```
## usage
```python
from easy_pika.simple.consumer import EasyConsumer
from easy_pika.simple.producer import EasyProducer


def test_producer():
    producer = EasyProducer("localhost", 5672)
    producer.declare_queue("simple_queue", durable=True)
    producer.publish("hello world", durable=True)
    
def test_consumer():
    def callback(ch, method, properties, body):
        print(body)
        
    consumer = EasyConsumer("localhost", 5672)
    consumer.declare_queue("simple_queue", durable=True)
    consumer.consume(callback=callback, auto_ack=True)
```