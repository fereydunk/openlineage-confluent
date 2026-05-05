#!/usr/bin/env python3
import os, sys, time
topic  = sys.argv[1]
group  = sys.argv[2]
try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    sys.exit(0)
cfg = {
    "bootstrap.servers":  os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol":  "SASL_SSL",
    "sasl.mechanism":     "PLAIN",
    "sasl.username":      os.environ["KAFKA_API_KEY"],
    "sasl.password":      os.environ["KAFKA_API_SECRET"],
    "group.id":           group,
    "auto.offset.reset":  "latest",
    "enable.auto.commit": True,
}
c = Consumer(cfg)
c.subscribe([topic])
while True:
    msg = c.poll(timeout=5.0)
    if msg is None:
        continue
    if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
        time.sleep(1)
