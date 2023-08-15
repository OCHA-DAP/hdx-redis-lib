[![Run tests](https://github.com/OCHA-DAP/hdx-redis-lib/actions/workflows/run-tests.yml/badge.svg)](https://github.com/OCHA-DAP/hdx-redis-lib/actions/workflows/run-tests.yml)

HDX REDIS LIB
=============

Simplifying access to redis streams and redis key-value store

Example usage for waiting for events from the "event bus" (from redis streams):
-------------------------------------------------------------------------------

```python
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

stream_name = 'hdx_event_stream'
group_name = 'default_group'
consumer_name = 'consumer-1' 

event_bus = connect_to_hdx_event_bus(
    stream_name, group_name, consumer_name,
    RedisConfig(host='redis', db=7)
)

while True:
    # Read events from the stream starting from the last unread
    event = event_bus.read_hdx_event()
    if event:
        # do something with the event
        pass
```

Example usage for storing and retrieving key/values
---------------------------------------------------

```python
from hdx_redis_lib import RedisKeyValueStore, RedisConfig


kv_store = RedisKeyValueStore(RedisConfig(host='redis', db=7))

# Storing / retrieving strings
kv_store.set_string('key1', 'value1')
kv_store.get_string('key1') # prints 'value1'

# Storing / retrieving dicts
kv_store.set_object('key2', {'dict_key1': 'some value', 'dict_key2': 'another_value'})
kv_store.get_object('key2') # prints {'dict_key1': 'some value', 'dict_key2': 'another_value'}
```

Example usage for storing and retrieving sets and maps
------------------------------------------------------

```python
from hdx_redis_lib import RedisKeyValueStore, RedisConfig


kv_store = RedisKeyValueStore(RedisConfig(host='redis', db=7))

# Storing / retrieving sets
set_name = 'set1'
kv_store.set_set(set_name, {'val1', 'val2', 'val3'})
print(kv_store.is_in_set('set1', 'val2')) # prints True
print(kv_store.get_set(set_name)) # prints {'val1', 'val2', 'val3'}

# Storing / retrieving maps
map_name = 'map1'
map1 = {
    'key1': 'value1',
    'key2': 'value2'
}
kv_store.set_map(map_name, map1)
print(kv_store.get_map(map_name)) # prints {'key1': 'value1', 'key2': 'value2'}
print(kv_store.get_map_value(map_name, 'key1')) # prints 'value1'
print(kv_store.get_map_and_delete(map_name)) # prints {'key1': 'value1', 'key2': 'value2'}
print(kv_store.get_map_and_delete(map_name)) # prints {}
print(kv_store.get_map_value(map_name, 'key1')) # prints None
```