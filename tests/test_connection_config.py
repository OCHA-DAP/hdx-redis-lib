from hdx_redis_lib.redis_connections import (
    RedisConfig,
    WriteOnlyRedisEventBus,
    RedisKeyValueStore,
    DEFAULT_SOCKET_TIMEOUT,
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_READ_BLOCK_MS,
)


def _connection_kwargs(redis_conn):
    return redis_conn.connection_pool.connection_kwargs


def test_default_socket_timeout_exceeds_read_block():
    # The finite default must stay above the blocking-read window, otherwise
    # redis-py >= 8 aborts the blocking XREADGROUP before the server responds.
    assert DEFAULT_SOCKET_TIMEOUT * 1000 > DEFAULT_READ_BLOCK_MS


def test_event_bus_applies_default_timeouts():
    bus = WriteOnlyRedisEventBus('some-stream', RedisConfig(host='localhost', db=0))
    kwargs = _connection_kwargs(bus.redis_conn)
    assert kwargs['socket_timeout'] == DEFAULT_SOCKET_TIMEOUT
    assert kwargs['socket_connect_timeout'] == DEFAULT_SOCKET_CONNECT_TIMEOUT


def test_key_value_store_applies_default_timeouts():
    store = RedisKeyValueStore(RedisConfig(host='localhost', db=0))
    kwargs = _connection_kwargs(store.redis_conn)
    assert kwargs['socket_timeout'] == DEFAULT_SOCKET_TIMEOUT
    assert kwargs['socket_connect_timeout'] == DEFAULT_SOCKET_CONNECT_TIMEOUT


def test_socket_timeout_can_be_overridden():
    bus = WriteOnlyRedisEventBus(
        'some-stream', RedisConfig(host='localhost', db=0, socket_timeout=None)
    )
    assert _connection_kwargs(bus.redis_conn)['socket_timeout'] is None

    bus2 = WriteOnlyRedisEventBus(
        'some-stream', RedisConfig(host='localhost', db=0, socket_timeout=30)
    )
    assert _connection_kwargs(bus2.redis_conn)['socket_timeout'] == 30
