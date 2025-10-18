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
        self.queue_name = ""

    def declare_queue(self, queue_name: str, durable: bool = False):
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=durable)

    def consume(self, callback, auto_ack: bool = True):
        assert self.queue_name, "declare queue before consuming"
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )

    def start_consuming(self):
        self.channel.start_consuming()