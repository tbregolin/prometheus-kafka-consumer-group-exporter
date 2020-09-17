import time

from .metrics import METRIC_PREFIX

from prometheus_client import Summary

REQUEST_TIME = Summary(METRIC_PREFIX + 'exporter_kafka_request_duration', 'Duration of requests to Kafka brokers', ('replica', 'request'))

class KafkaAsyncRequest:
    def __init__(self, client, node, request, callback, *args, **kwargs):
        self._client = client
        self._node = node
        self._request = request
        self._callback = callback
        self._args = args
        self._kwargs = kwargs

    def send(self):
        self._start_ts = time.monotonic()

        f = self._client.send(self._node, self._request)
        f.add_callback(self.callback, *self._args, **self._kwargs)

    def callback(self, *args, **kwargs):
        self._end_ts = time.monotonic()
        REQUEST_TIME.labels(self._node, type(self._request).__name__).observe(self._end_ts - self._start_ts)

        self._callback(*args, **kwargs)
