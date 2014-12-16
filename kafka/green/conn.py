import gevent.socket as socket
import logging

from kafka.conn import KafkaConnection

log = logging.getLogger("kafka")

class _KafkaConnection(KafkaConnection):
    """
    Gevent version of kafka.KafkaConnection class. Uses
    gevent.socket instead of socket.socket.
    """
    def __init__(self, host, port, timeout=10):
        super(_KafkaConnection, self).__init__(host, port, timeout)

    def reinit(self):
        """
        Re-initialize the socket connection
        """
        log.debug("Reinitializing socket connection for %s:%d" % (self.host, self.port))

        if self._sock:
            self.close()

        try:
            self._sock = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error:
            log.exception('Unable to connect to kafka broker at %s:%d' % (self.host, self.port))
            self._raise_connection_error()
