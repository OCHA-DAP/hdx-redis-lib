import json
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable, Tuple

import redis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)


@dataclass
class RedisConfig:
    host: str
    db: int
    port: int = 6379


class WriteOnlyRedisEventBus:
    def __init__(self, stream_name: str, redis_conf: RedisConfig, max_len=50_000):
        self.redis_conn = redis.Redis(host=redis_conf.host, port=redis_conf.port, db=redis_conf.db)
        self.stream_name = stream_name
        self.max_len = max_len

    def push_event(self, event: Dict[str, Any]) -> None:
        self.redis_conn.xadd(self.stream_name, event, maxlen=self.max_len)

    def config_max_len(self, max_len: int):
        self.max_len = max_len
        return self


class HDXWriteOnlyRedisEventBus(WriteOnlyRedisEventBus):
    def push_hdx_event(self, event: Dict[str, Any]) -> None:
        processed_event = {
            'event_type': event['event_type'],
            'body': bytes(json.dumps(event), 'utf-8')
        }
        self.push_event(processed_event)


class RedisEventBus():
    def __init__(self, stream_name: str, group_name: str, consumer_name: str, redis_conf: RedisConfig) -> None:
        self._write_only_event_bus = self._create_write_only_event_bus(stream_name, redis_conf)
        self.redis_conn = self._write_only_event_bus.redis_conn
        self.stream_name = self._write_only_event_bus.stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name

    def _create_write_only_event_bus(self, stream_name: str, redis_conf: RedisConfig) -> WriteOnlyRedisEventBus:
        return WriteOnlyRedisEventBus(stream_name, redis_conf)

    def config_max_len(self, max_len: int):
        self._write_only_event_bus.config_max_len(max_len)
        return self

    def push_event(self, data: Dict[str, Any]) -> None:
        self._write_only_event_bus.push_event(data)

    def read_event(self, block: Optional[int] = 2 * 60 * 1000) -> Optional[Dict[str, str]]:
        try:
            logger.debug('Trying to create consumer group {}'.format(self.group_name))
            self.redis_conn.xgroup_create(self.stream_name, self.group_name, id='0', mkstream=True)
        except ResponseError as re:
            if 'BUSYGROUP' in re.args[0]:
                logger.debug('Consumer group {} already exists'.format(self.consumer_name))
            else:
                raise re
        result = self.redis_conn.xreadgroup(
            groupname=self.group_name, consumername=self.consumer_name, streams={self.stream_name: '>'},
            block=block, count=1, noack=True
        )

        if not result:
            return None

        stream_name, events = result[0]
        event_id, event_data = events[0]
        self.redis_conn.xack(self.stream_name, self.group_name, event_id)

        decoded_result = {k.decode('utf-8'): v.decode('utf-8') for k, v in event_data.items()}

        return decoded_result

    def listen(self, event_processor: Callable[[Dict], Tuple[bool, str]],
               event_transformer: Callable[[Dict], Optional[Dict]] = None):
        try:
            while True:
                event = self.read_event()
                if event_transformer:
                    event = event_transformer(event)
                if not event:
                    logger.info('Event with value None arrived')
                else:
                    result, message = event_processor(event)
                    if not result:
                        logger.error('Event processor decided to stop. Reason: {}'.format(message))
        except Exception as e:
            logger.error(str(e))


class HDXRedisEventBus(RedisEventBus):

    def _create_write_only_event_bus(self, stream_name: str, redis_conf: RedisConfig) -> WriteOnlyRedisEventBus:
        return HDXWriteOnlyRedisEventBus(stream_name, redis_conf)

    def read_hdx_event(self, block: Optional[int] = 2 * 60 * 1000) -> Optional[Dict[str, str]]:
        generic_event = self.read_event(block=block)
        return _extract_hdx_event(generic_event)

    def hdx_listen(self, event_processor: Callable[[Dict], Tuple[bool, str]]):
        return self.listen(event_processor, event_transformer=_extract_hdx_event)


def _extract_hdx_event(generic_event: Dict) -> Optional[Dict]:
    event_body_json = generic_event.get('body') if generic_event else None
    if event_body_json:
        event_body = json.loads(event_body_json)
        return event_body
    return None

class RedisKeyValueStore:
    def __init__(self, redis_conf: RedisConfig, expire_in_seconds=12*60*60) -> None:
        self.redis_conn = redis.Redis(host=redis_conf.host, port=redis_conf.port, db=redis_conf.db)
        self.expire_in_seconds = expire_in_seconds

    def set_string(self, key: str, value: str) -> None:
        encoded_value = bytes(value, 'utf-8')
        self.redis_conn.set(key, encoded_value, ex=self.expire_in_seconds)

    def get_string(self, key: str) -> Optional[str]:
        value = self.redis_conn.get(key)
        return value.decode('utf-8') if value is not None else None

    def set_object(self, key: str, value: Any) -> None:
        encoded_value = bytes(json.dumps(value), 'utf-8')
        self.redis_conn.set(key, encoded_value, ex=self.expire_in_seconds)

    def get_object(self, key: str) -> Optional[Any]:
        value = self.redis_conn.get(key)
        decoded_value = value.decode('utf-8') if value is not None else None
        return json.loads(decoded_value)


def connect_to_write_only_event_bus(stream_name: str, redis_conf: RedisConfig = None) -> WriteOnlyRedisEventBus:
    """
    Creates and returns an event bus that can only be used for writing

    Parameters:
    - `stream_name` (str): The name of the stream.
    - `redis_conf` (RedisConfig): An instance of the `RedisConfig` class (default is `None`).

    Returns:
    - An event bus to which one can only push data.
    """
    event_bus = WriteOnlyRedisEventBus(stream_name, redis_conf)
    return event_bus


def connect_to_hdx_write_only_event_bus(stream_name: str, redis_conf: RedisConfig = None) -> HDXWriteOnlyRedisEventBus:
    """
    Creates and returns an event bus that can only be used for writing

    Parameters:
    - `stream_name` (str): The name of the stream.
    - `redis_conf` (RedisConfig): An instance of the `RedisConfig` class (default is `None`).

    Returns:
    - An event bus to which one can only push data. It has the additional push_hdx_event() method.
    """
    event_bus = HDXWriteOnlyRedisEventBus(stream_name, redis_conf)
    return event_bus


def connect_to_event_bus(stream_name: str, group_name: str, consumer_name: str,
                         redis_conf: RedisConfig = None) -> RedisEventBus:
    event_bus = RedisEventBus(stream_name, group_name, consumer_name, redis_conf)
    return event_bus


def connect_to_hdx_event_bus(stream_name: str, group_name: str, consumer_name: str,
                             redis_conf: RedisConfig = None) -> HDXRedisEventBus:
    """
    Creates and returns an instance of `HDXRedisEventBus`.

    Parameters:
    - `stream_name` (str): The name of the Redis stream.
    - `group_name` (str): The name of the Redis consumer group.
    - `consumer_name` (str): The name of the Redis consumer.
    - `redis_conf` (RedisConfig): An instance of the `RedisConfig` class (default is `None`).

    Returns:
    - An instance of `HDXRedisEventBus`.
    """
    event_bus = HDXRedisEventBus(stream_name, group_name, consumer_name, redis_conf)
    return event_bus