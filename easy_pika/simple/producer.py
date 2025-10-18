import pika
from pika import BasicProperties


class EasyProducer:
    def __init__(self, channel):
        try:
            self.connection = pika.BlockingConnection()
            self.channel = channel

            
    def publish(self, exchange, routing_key, body):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body
        )