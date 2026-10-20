"""Small helpers around confluent-kafka used by master, workers and scripts."""
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config


def make_producer(extra=None):
    conf = {
        "bootstrap.servers": config.BOOTSTRAP,
        "message.max.bytes": config.MAX_MESSAGE_BYTES,
        "enable.idempotence": True,      # no duplicate writes on retry, implies acks=all
    }
    conf.update(extra or {})
    return Producer(conf)


def make_consumer(group_id, extra=None):
    conf = {
        "bootstrap.servers": config.BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,     # we commit manually AFTER the work is done
        "auto.offset.reset": "earliest",
        "fetch.message.max.bytes": config.MAX_MESSAGE_BYTES,
        "session.timeout.ms": 10000,     # a crashed consumer is detected after ~10 s
    }
    conf.update(extra or {})
    return Consumer(conf)


def headers_to_dict(headers):
    out = {}
    for k, v in headers or []:
        out[k] = v.decode() if isinstance(v, (bytes, bytearray)) else v
    return out


def dict_to_headers(d):
    return [(k, str(v).encode()) for k, v in d.items()]


def ensure_topics():
    """Create tasks/results/heartbeats if missing (safe to call many times).
    Returns the list of topic names that were newly created."""
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    week = str(7 * 24 * 3600 * 1000)
    big = str(config.MAX_MESSAGE_BYTES)
    wanted = [
        NewTopic(config.TOPIC_TASKS, config.TASKS_PARTITIONS, 1,
                 config={"max.message.bytes": big, "retention.ms": week}),
        NewTopic(config.TOPIC_RESULTS, config.TASKS_PARTITIONS, 1,
                 config={"max.message.bytes": big, "retention.ms": week}),
        NewTopic(config.TOPIC_HEARTBEATS, 1, 1,
                 config={"retention.ms": str(3600 * 1000)}),
    ]
    to_create = [t for t in wanted if t.topic not in existing]
    created = []
    if to_create:
        for name, fut in admin.create_topics(to_create).items():
            try:
                fut.result()
                created.append(name)
            except Exception as e:  # another process may have created it first
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    raise
    return created


def describe_topics():
    """Return {topic: [(partition, leader, replicas, isr), ...]} for our 3 topics."""
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP})
    md = admin.list_topics(timeout=10)
    out = {}
    for name in (config.TOPIC_TASKS, config.TOPIC_RESULTS, config.TOPIC_HEARTBEATS):
        t = md.topics.get(name)
        if not t:
            continue
        out[name] = [(p.id, p.leader, list(p.replicas), list(p.isrs))
                     for p in sorted(t.partitions.values(), key=lambda p: p.id)]
    return out
