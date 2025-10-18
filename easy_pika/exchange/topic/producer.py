import pika
from pika import BlockingConnection
from pika import ConnectionParameters
from pika.exceptions import AMQPConnectionError
from loguru import logger


class EasyProducer:
    def __init__(self, rabbitmq_host: str, rabbitmq_port: int = 5672):
        try:
            self.connection = BlockingConnection(
                parameters=ConnectionParameters(
                    host=rabbitmq_host,
                    port=rabbitmq_port,
                )
            )
            self.channel = self.connection.channel()
        except AMQPConnectionError as e:
            logger.error("rabbitmq connection error: {}".format(e))
        except Exception as e:
            logger.error("unknown connection error: {}".format(e))
        self.exchange_name = ""

    def declare_exchange(self, exchange_name: str, durable: bool = False):
        self.exchange_name = exchange_name
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="topic",
            durable=durable
        )

    def publish(self, body, routing_key: str, durable: bool = False):
        assert self.exchange_name, "declare exchange before publishing"
        if durable:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                )
            )
        else:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=body
            )