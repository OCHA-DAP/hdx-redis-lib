import json
import logging
import os

from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable, Tuple, Iterable, Set

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
               pre_filter: Callable[[Dict], bool] = None,
               event_transformer: Callable[[Dict], Optional[Dict]] = None,
               max_iterations: int = None):
        """
        Listens to and processes incoming events. The function runs in an infinite loop (unless max_iterations is
        specified until ) an exception occurs or the event_processor returns a False result.

        Parameters:
        - `event_processor` : Callable[[Dict], Tuple[bool, str]]
            A function to process the event. It should take a dictionary as input and return
            a tuple where the first element is a boolean indicating the success of the operation
            and the second is a string message which will be logged if not successful
        - `pre_filter` : Callable[[Dict], bool], optional
            A function that takes an event as input and returns a boolean value indicating whether
            the event should be processed. Default is None.
        - `event_transformer` : Callable[[Dict], Optional[Dict]], optional
            A function that transforms the event into something that the event processor function expects to
            receive. Ex: JSON string to Dict
        - `max_iterations` : Max number of iterations. After that the function just exits.

        """
        counter = 1
        try:
            while True:
                if max_iterations and max_iterations > 0:
                    if max_iterations < counter:
                        logger.info('Exiting listening function because max iteration was reached')
                        break
                    counter += 1
                event = self.read_event()
                if event and pre_filter:
                    event = event if pre_filter(event) else None
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

    def hdx_listen(self, event_processor: Callable[[Dict], Tuple[bool, str]],
                   allowed_event_types: Iterable[str] = None,
                   max_iterations: int = None):

        if allowed_event_types:
            return self.listen(event_processor,
                               pre_filter=lambda event: _filter_by_event_type(event, allowed_event_types),
                               event_transformer=_extract_hdx_event,
                               max_iterations=max_iterations
                               )
        else:
            return self.listen(event_processor,
                               event_transformer=_extract_hdx_event,
                               max_iterations=max_iterations
                               )


def _extract_hdx_event(generic_event: Dict) -> Optional[Dict]:
    event_body_json = generic_event.get('body') if generic_event else None
    if event_body_json:
        event_body = json.loads(event_body_json)
        return event_body
    return None


def _filter_by_event_type(event: Dict, allowed_event_types: Iterable[str]) -> bool:
    if event and event.get('event_type') in allowed_event_types:
        return True
    return False


class RedisKeyValueStore:
    def __init__(self, redis_conf: RedisConfig, expire_in_seconds=12*60*60) -> None:
        self.redis_conn = redis.Redis(host=redis_conf.host, port=redis_conf.port, db=redis_conf.db)
        self.expire_in_seconds = expire_in_seconds

    def exists(self, key):
        return self.redis_conn.exists(key)

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
        return json.loads(decoded_value) if decoded_value else decoded_value

    def set_set(self, key: str, values: Set[str]):
        encoded_values = [bytes(v, 'utf-8') for v in values]
        length = len(encoded_values)
        step = 200
        start = 0
        for i in range(start, length, step):
            chunk = encoded_values[i:i+step]
            self.redis_conn.sadd(key, *chunk)
        self.redis_conn.expire(key, self.expire_in_seconds)

    def get_set(self, key: str) -> Set[str]:
        values = self.redis_conn.smembers(key)
        if values:
            decoded_values = {v.decode('utf-8') for v in values}
            return decoded_values
        return values

    def is_in_set(self, key: str, value: str) -> bool:
        encoded_value = bytes(value, 'utf-8')
        result = self.redis_conn.sismember(key, encoded_value)
        return result

    def set_map(self, key: str, map_dict: Dict[str, str]):
        encoded_map = {bytes(k, 'utf-8'): bytes(v, 'utf-8') for k, v in map_dict.items()}
        self.redis_conn.hset(key, mapping=encoded_map)

    def get_map(self, key: str):
        map_dict = self.redis_conn.hgetall(key)
        if map_dict:
            decoded_map = {k.decode('utf-8'): v.decode('utf-8') for k, v in map_dict.items()}
            return decoded_map
        return map_dict

    def get_map_and_delete(self, key: str):
        pipe = self.redis_conn.pipeline()
        pipe.hgetall(key)
        pipe.delete(key)
        result = pipe.execute()
        if not result:
            logger.warning(f'Map {key} could not be fetched and deleted')
            return None
        else:
            map_dict = result[0]
            decoded_map = {k.decode('utf-8'): v.decode('utf-8') for k, v in map_dict.items()}
            return decoded_map

    def get_map_value(self, key: str, map_key: str):
        encoded_map_key = bytes(map_key, 'utf-8')
        value = self.redis_conn.hget(key, encoded_map_key)
        return value.decode('utf-8') if value is not None else None


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


def connect_to_hdx_event_bus_with_env_vars() -> HDXRedisEventBus:
    """
    Expects the following env vars:
    - REDIS_STREAM_HOST (if missing assumes 'redis')
    - REDIS_STREAM_PORT (if missing assumes 6379)
    - REDIS_STREAM_DB (if missing assumes 7)
    - REDIS_STREAM_STREAM_NAME
    - REDIS_STREAM_GROUP_NAME
    - REDIS_STREAM_CONSUMER_NAME

    Returns:
    --------
    - An instance of `HDXRedisEventBus`.
    """
    redis_stream_host = os.getenv('REDIS_STREAM_HOST', 'redis')
    redis_stream_port = os.getenv('REDIS_STREAM_PORT', 6379)
    redis_stream_db = os.getenv('REDIS_STREAM_DB', 7)

    # Define the stream name and the ID to start reading from
    stream_name = os.getenv('REDIS_STREAM_STREAM_NAME', None)
    group_name = os.getenv('REDIS_STREAM_GROUP_NAME', None)
    consumer_name = os.getenv('REDIS_STREAM_CONSUMER_NAME', None)

    if not stream_name or not group_name or not consumer_name:
        raise ValueError(
            'One of the following env variables is missing: REDIS_STREAM_STREAM_NAME, REDIS_STREAM_GROUP_NAME, '
            'REDIS_STREAM_CONSUMER_NAME'
        )

    return connect_to_hdx_event_bus(stream_name, group_name, consumer_name,
                                    RedisConfig(host=redis_stream_host, port=redis_stream_port, db=redis_stream_db))


def connect_to_key_value_store_with_env_vars(expire_in_seconds=12*60*60) -> RedisKeyValueStore:
    """
    Expects the following env vars:
    - REDIS_STREAM_HOST (if missing assumes 'redis')
    - REDIS_STREAM_PORT (if missing assumes 6379)
    - REDIS_STREAM_DB (if missing assumes 7)

    Returns:
    --------
    - An instance of `RedisKeyValueStore`.
    """
    redis_stream_host = os.getenv('REDIS_STREAM_HOST', 'redis')
    redis_stream_port = os.getenv('REDIS_STREAM_PORT', 6379)
    redis_stream_db = os.getenv('REDIS_STREAM_DB', 7)

    return RedisKeyValueStore(RedisConfig(host=redis_stream_host, db=redis_stream_db, port=redis_stream_port),
                              expire_in_seconds=expire_in_seconds)
