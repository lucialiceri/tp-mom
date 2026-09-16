import pika
import random
import string
from .middleware import MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        # Queue declaration
        self.queue_name = queue_name
        self.queue_declaration = self.channel.queue_declare(queue=queue_name)

        self.consumer_tag = None
        

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
 

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            # Functions needed for on_message_callback
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        # When a message is recieved
        self.consumer_tag =self.channel.basic_consume(
            queue = self.queue_name, 
            on_message_callback=callback
        )

        self.channel.start_consuming()

    def stop_consuming(self):
        try:
            if self.consumer_tag:
                self.channel.basic_cancel(consumer_tag=self.consumer_tag)
            self.channel.stop_consuming()

        except pika.exceptions.ConnectionClosed as e:
            raise MessageMiddlewareDisconnectedError(e)

        except Exception as e:
            raise MessageMiddlewareDisconnectedError(e)

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys= routing_keys
       
    def start_consuming(self, on_message_callback):
        pass

    def send(self, message):
         self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)
    
    def stop_consuming(self):
        pass

    def close(self):
        pass
