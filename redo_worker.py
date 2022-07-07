import asyncio
import time
from threading import Event
import signal

from libs.utils import get_logger
from libs.senzing_libs.senzing_init import SenzingInit


class Worker(SenzingInit):

    def __init__(self, ex_ev):
        self.ex_ev = ex_ev
        self.log = get_logger()
        super(Worker, self).__init__(self.log)
        self.threads = 5

    async def work(self):

        while not self.ex_ev.is_set():
            cnt = self.g2_engine.countRedoRecords()
            if not cnt:
                await asyncio.sleep(10)
                continue

            response_bytearray = bytearray()
            try:
                self.g2_engine.processRedoRecord(response_bytearray)
            except Exception as e:
                self.log.error(e)
                active_config_id_bytearray = bytearray()
                default_config_id_bytearray = bytearray()
                self.g2_engine.getActiveConfigID(active_config_id_bytearray)
                self.g2_config_mgr.getDefaultConfigID(
                    default_config_id_bytearray)
                try:
                    if active_config_id_bytearray != \
                            default_config_id_bytearray:
                        self.g2_engine.reinit(default_config_id_bytearray)
                        self.log.info('G2Engine reinitialised')
                except Exception as err:
                    self.log.error(err)

        if self.ex_ev.is_set():
            self.log.info('Stopping daemons.')

    def run(self):
        loop = asyncio.get_event_loop()
        tasks = [loop.create_task(self.work()) for _ in
                 range(self.threads)]
        group = asyncio.gather(*tasks)
        loop.run_until_complete(group)


if __name__ == '__main__':
    time.sleep(10)
    exit_event = Event()
    app = Worker(exit_event)


    def signal_handler(sig, frame):
        app.ex_ev.set()


    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    app.run()
