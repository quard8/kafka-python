from kafka.producer.base import Producer, _send_upstream, STOP_ASYNC_PRODUCER
from kafka.producer.simple import SimpleProducer
from kafka.producer.keyed import KeyedProducer

import gevent
from gevent.queue import Queue


class _ProducerMixin(object):

    def _setup_async(self, batch_send_every_t, batch_send_every_n):
        self.queue = Queue()  # Messages are sent through this queue
        self.proc = gevent.spawn(_send_upstream,
                                 self.queue,
                                 self.client.copy(),
                                 self.codec,
                                 batch_send_every_t,
                                 batch_send_every_n,
                                 self.req_acks,
                                 self.ack_timeout)

    def stop(self, timeout=1):
        if self.async:
            self.queue.put((STOP_ASYNC_PRODUCER, None))
            self.proc.join(timeout)
            if self.proc.dead is False:
                self.proc.kill()


class _Producer(_ProducerMixin, Producer):
    pass


class _SimpleProducer(_ProducerMixin, SimpleProducer):
    pass


class _KeyedProducer(_ProducerMixin, KeyedProducer):
    pass
