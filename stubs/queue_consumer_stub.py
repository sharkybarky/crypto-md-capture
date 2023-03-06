from logging import getLogger

log = getLogger(__name__)


class StubConsumer:
    def __init__(self):
        pass

    def consume(self, trade_queue, shutdown_event):
        while not shutdown_event.is_set() or trade_queue.qsize() > 0:
            message = trade_queue.get()
            log.info(f"Consumer read message: {message}, queue-size: {trade_queue.qsize()}")

        log.info(f"Stub consumer exiting")
