from hdx_redis_lib import connect_to_key_value_store_with_env_vars

KEY = 'redis-test-key'


def test_string_storage():
    test_string = 'ßräüëÉç_Üß€öä_úï'
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.set_string(KEY, test_string)
    retrieved_string = key_value_store.get_string(KEY)
    assert test_string == retrieved_string


def test_object_storage():
    test_object = {
        'key1': 'value1',
        'key2': 'value2',
        'nested_object': {
            'special_chars': 'ßräüëÉç_Üß€öä_úï'
        }
    }
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.set_object(KEY, test_object)
    retrieved_object = key_value_store.get_object(KEY)
    assert test_object == retrieved_object


def test_set_storage():
    test_set = {'val1', 'val2', 'ßräüëÉç_Üß€öä_úï'}
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.set_set(KEY, test_set)
    assert key_value_store.is_in_set(KEY, 'ßräüëÉç_Üß€öä_úï')
    assert not key_value_store.is_in_set(KEY, 'another_value')
    retrieved_set = key_value_store.get_set(KEY)
    assert retrieved_set == test_set


def test_map_storage():
    test_value = 'value_ßräüëÉç_Üß€öä_úï'
    test_map = {
        'key1': 'value1',
        'key2_ßräüëÉç_Üß€öä_úï': test_value
    }
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.set_map(KEY, test_map)
    retrieved_value = key_value_store.get_map_value(KEY, 'key2_ßräüëÉç_Üß€öä_úï')
    assert retrieved_value == test_value
    assert not key_value_store.get_map_value(KEY, 'another_key')
    retrieved_map = key_value_store.get_map(KEY)
    assert retrieved_map == test_map
    retrieved_map2 = key_value_store.get_map_and_delete(KEY)
    assert retrieved_map2 == retrieved_map
    assert not key_value_store.get_map(KEY)
