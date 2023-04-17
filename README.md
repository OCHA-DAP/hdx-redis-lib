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
kv_store.get_string('key1') # will return 'value1'

# Storing / retrieving dicts
kv_store.set_object('key2', {'dict_key1': 'some value', 'dict_key2': 'another_value'})
kv_store.get_object('key2') # will return {'dict_key1': 'some value', 'dict_key2': 'another_value'}
```