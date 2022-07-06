import time
import json
import asyncio
from asyncio import Event
import signal
import aio_pika

from libs.utils import get_logger
from libs.slack import send_alert
from libs.conf import (RABBIT_Q, RABBIT_Q_ERROR, RABBIT_PASS, RABBIT_USER,
                       RABBIT_URL)
from libs.senzing_libs.senzing_init import SenzingInit


class Worker(SenzingInit):

    def __init__(self, ex_ev):
        self.ex_ev = ex_ev
        self.log = get_logger()
        self.q = RABBIT_Q
        self.q_err = RABBIT_Q_ERROR
        super(Worker, self).__init__(self.log)
        self.threads = 5
        self.loop = asyncio.get_event_loop()
        self.config = {
            'queue_host': RABBIT_URL,
            'queue_port': 5672,
            'queue_vhost': '/',
            'queue_user': RABBIT_USER,
            'queue_pass': RABBIT_PASS
        }

    async def publish(self, task, q):
        connection = await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASS}@{RABBIT_URL}/",
        )
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(task).encode()),
                routing_key=q
            )

    async def requeue_task(self, task, err):
        await asyncio.sleep(10)
        if not task:
            return
        if 'tries' in task:
            task['tries'] += 1
        else:
            task['tries'] = 1

        task['error'] = repr(err)

        if task['tries'] < 5:
            self.log.debug('Requeue task after failed')
            await self.publish(task, self.q)
        else:
            self.log.warning('All tries failed. Sent task to error queue.')
            await self.publish(task, self.q_err)
            self.log.warning('All tries failed. Removed temp dir.')
            send_alert(f'{self.__class__.__name__} Error!',
                       f'Error: {err}', 'E')

    async def process(self, str_data):
        json_data = json.loads(str_data)
        try:
            self.g2_engine.addRecord(
                json_data["DATA_SOURCE"],
                json_data["RECORD_ID"],
                str_data.decode())

        except Exception as e:
            self.log.error(e)
            active_config_id_bytearray = bytearray()
            default_config_id_bytearray = bytearray()
            self.g2_engine.getActiveConfigID(active_config_id_bytearray)
            self.g2_config_mgr.getDefaultConfigID(default_config_id_bytearray)
            try:
                if active_config_id_bytearray != default_config_id_bytearray:
                    self.g2_engine.reinit(default_config_id_bytearray)
                    self.log.info('G2Engine reinitialised')
                    data_as_json = json.loads(str_data)
                    try:
                        self.g2_engine.addRecord(
                            data_as_json["DATA_SOURCE"],
                            data_as_json["RECORD_ID"],
                            str_data.decode())
                    except Exception as e:
                        self.log.error(e)

            except Exception as err:
                self.log.error(err)

    async def work(self):

        connection = await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASS}@{RABBIT_URL}/",
        )

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=100)
            queue = await channel.declare_queue(self.q, auto_delete=False,
                                                durable=True)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if self.ex_ev.is_set():
                        return
                    try:
                        await self.process(message.body)
                        await message.ack()
                    except Exception as e:
                        self.log.exception(e)
                        await self.requeue_task(json.loads(message.body), e)

        if self.ex_ev.is_set():
            self.log.info('Stopping daemons.')

    def run(self):
        task = self.loop.create_task(self.work())
        self.loop.run_until_complete(task)


if __name__ == '__main__':
    # wait rabbit start
    time.sleep(10)
    exit_event = Event()
    app = Worker(exit_event)


    def signal_handler(sig, frame):
        app.ex_ev.set()
        app.save_config()


    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    app.run()
