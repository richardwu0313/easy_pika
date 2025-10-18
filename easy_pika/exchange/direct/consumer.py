import pika
from pika import BlockingConnection
from pika import ConnectionParameters
from pika.exceptions import AMQPConnectionError
from loguru import logger


class EasyConsumer:
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
        self.queue_name = ""

    def declare_exchange(self, exchange_name: str, durable: bool = False):
        self.exchange_name = exchange_name
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=durable
        )

    def declare_queue(self, queue_name: str, durable: bool = False):
        if not queue_name:
            result = self.channel.queue_declare(
                queue="",
                durable=durable,
                exclusive=True)
            self.queue_name = result.method.queue
        else:
            self.queue_name = queue_name
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=durable,
            )

    def bind_queue(self, routing_keys: list[str]):
        assert self.exchange_name, "declare exchange before binding"
        assert self.queue_name, "declare queue before binding"
        for key in routing_keys:
            self.channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=key,
            )

    def consume(self, callback, auto_ack: bool = True):
        assert self.queue_name, "declare queue before consuming"
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )

    def start_consuming(self):
        self.channel.start_consuming()