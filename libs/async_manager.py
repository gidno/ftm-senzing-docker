"""
Contain class to simplified work with RabbitMQ queues.
"""
import asyncio
import json
from warnings import warn
import datetime
from asyncio import AbstractEventLoop
from typing import Dict, Optional, Any, AsyncGenerator, Union
from aio_pika.pool import Pool
from aio_pika import (Message, Connection, Channel, Queue, connect_robust,
                      IncomingMessage)
from .utils import get_host_name


class AIOQueueManager:
    """
    Class to simplified work with RabbitMQ queues.
    """

    __slots__ = ("loop", "config", "__pool", "exchange", "__channel_pool",
                 "ex_ev", "__weakref__", "internal_queue", "amqp_queue",
                 "consumer_tag")

    def __init__(
        self,
        loop: AbstractEventLoop,
        config: Dict[Any, Any],
        exchange: Optional[str] = None,
        pool_size: int = 10,
        channel_pool_size: int = 10,
        ex_ev=None
    ):

        self.loop = loop
        self.config = config
        self.pool = pool_size
        self.exchange = exchange or ''
        self.channel_pool = channel_pool_size
        self.ex_ev = ex_ev
        self.internal_queue = asyncio.Queue()
        self.amqp_queue = None
        self.consumer_tag = None

    async def get_connection(self) -> Connection:
        """
        Create robust connection to RabbitMQ.
        :return:
        """
        return await connect_robust(
            host=self.config.get("queue_host", "localhost"),
            port=int(self.config.get("queue_port", 5672)),
            login=self.config.get("queue_user", "guest"),
            password=self.config.get("queue_pass", "guest"),
            virtualhost=self.config.get("queue_vhost", "/"),
        )

    @property
    def pool(self):
        """
        Create class attribute of class `pool`
        :return:
        """
        return self.__pool

    @pool.setter
    def pool(self, value):
        """
        Setter for class attribute `pool`
        :param value:
        :return:
        """
        self.__pool = Pool(self.get_connection, max_size=value, loop=self.loop)

    @property
    def channel_pool(self):
        """
        Create class attribute of class `channel_pool`
        :return:
        """
        return self.__channel_pool

    @channel_pool.setter
    def channel_pool(self, value):
        """
        Setter for class attribute `channel_pool`
        :param value:
        :return:
        """
        self.__channel_pool = Pool(
            self.get_channel, max_size=value, loop=self.loop
        )

    async def get_channel(self) -> Channel:
        """
        Get one connection from `pool` and create chanel.
        :return:
        """
        async with self.pool.acquire() as connection:
            return await connection.channel(publisher_confirms=False)

    async def publish(
        self,
        queue_name: str,
        message: Union[Dict[Any, Any], Message, str],
        create_queue: bool = False,
        priority: Optional[int] = None,
    ) -> None:
        """
        Publish message to destination queue.
        :param queue_name: destination queue
        :param message: Dict
        :param create_queue: If need to create queue if it don`t exist
        :param priority: If need create queue with priority.
        :return:
        """
        if isinstance(message, Message):
            message = json.loads(message.body)
        elif not isinstance(message, dict):
            message = json.loads(message)
        name, ip_addr = get_host_name()
        message.update({"data_sender": f"{name}({ip_addr})"})
        async with self.channel_pool.acquire() as channel:
            if create_queue:
                warn(
                    "In AIOQueueManager on publish do not "
                    "use 'create_queue' argument/flag."
                    "Instead use:"
                    "await AIOQueueManager().create_queue(<queue_name>)"
                )
                if priority is None:
                    await channel.declare_queue(
                        queue_name, durable=True, auto_delete=False
                    )
                else:
                    await channel.declare_queue(
                        queue_name,
                        durable=True,
                        auto_delete=False,
                        arguments={"x-max-priority": priority},
                    )
            await channel.default_exchange.publish(
                Message(body=json.dumps(message).encode()), queue_name
            )

    async def create_queue(
        self,
        name: str,
        priority: Optional[int] = None,
        exchange: Optional[str] = None,
    ) -> None:
        """
        Create queue under current exchange, if not set different exchange.
        By default queue without priority.
        :param name: [str] name of queue
        :param priority: [int] [1..6]
        :param exchange: [str] name of exchange to bind
        :return: None
        """
        async with self.channel_pool.acquire() as channel:
            if priority is None:
                queue: Queue = await channel.declare_queue(
                    name, durable=True, auto_delete=False
                )
            else:
                queue: Queue = await channel.declare_queue(
                    name,
                    durable=True,
                    auto_delete=False,
                    arguments={"x-max-priority": priority},
                )
            exchange = exchange or self.exchange
            await queue.bind(exchange, name)

    async def consume(self, consume_from: str) -> IncomingMessage:
        """
        Get one message from chosen queue.
        :param consume_from:
        :return:
        """
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(
                consume_from, durable=True, auto_delete=False
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    return message

    async def qsize(
        self, consume_from: str, priority: Optional[int] = False
    ) -> int:
        """
        Get a size of queue. If queue has priority need to set priority.
        :param consume_from:
        :param priority:
        :return:
        """
        async with self.channel_pool.acquire() as channel:
            if priority:
                queue = await channel.declare_queue(
                    consume_from,
                    durable=True,
                    auto_delete=False,
                    arguments={"x-max-priority": priority},
                )
            else:
                queue = await channel.declare_queue(
                    consume_from, durable=True, auto_delete=False
                )
            return queue.declaration_result.message_count

    async def delete(
        self, queue_name: str, if_unused: bool = False, if_empty: bool = False
    ) -> None:
        """
        Delete a chosen queue.
        :param queue_name:
        :param if_unused: del if queue unused
        :param if_empty: del queue if queue is empty.
        :return:
        """
        async with self.channel_pool.acquire() as channel:
            await channel.queue_delete(
                queue_name, if_unused=if_unused, if_empty=if_empty
            )

    async def consume_generator(
        self, consume_from: str, prefetch_count: int = 1, no_ack: bool = False
    ) -> AsyncGenerator[IncomingMessage, None]:
        """
        Create generator that consume messages from chosen queue.
        :param consume_from:
        :param prefetch_count: how many messages can be taken without
            acknowledge (each message is separate generator out)
        :param no_ack: if True messages automatically acknowledges
        :return:
        """
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=prefetch_count)
            queue = await channel.declare_queue(
                consume_from, durable=True, auto_delete=False
            )
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if no_ack:
                        message.ack()
                        yield message
                    else:
                        yield message

    async def internal_consume_generator(self, consume_from, prefetch_count=0,
                                         priority=None):
        """
        Unblocked generator that consume messages from internal queue.
        :param consume_from: queue name
        :param prefetch_count: how many messages can be taken without
            acknowledge (each message is separate generator out)
        :param priority: queue priority
        :return: message
        """
        async with self.channel_pool.acquire() as channel:
            if prefetch_count:
                await channel.set_qos(prefetch_count=prefetch_count)
            if priority:
                self.amqp_queue = await channel.declare_queue(
                    consume_from,
                    durable=True,
                    auto_delete=False,
                    arguments={"x-max-priority": priority},
                )
            else:
                self.amqp_queue = await channel.declare_queue(
                    consume_from, durable=True, auto_delete=False
                )
            # Start consuming
            self.consumer_tag = await self.amqp_queue.consume(
                self.internal_queue.put, no_ack=False)
            while not self.ex_ev.is_set():
                try:
                    yield self.internal_queue.get_nowait()
                except asyncio.queues.QueueEmpty:
                    await asyncio.sleep(1)
                    yield None
            else:
                return

    async def requeue_task(self, queue, err_queue, task, err):
        if 'tries' in task:
            task['tries'] += 1
        else:
            task['tries'] = 1
        if task['tries'] < 5:
            await self.publish(queue, task)
        else:
            task.update({'error': repr(err),
                         'daemon_name': self.config.get('name'),
                         'err_date': str(datetime.datetime.now())})
            await self.publish(err_queue, task)
