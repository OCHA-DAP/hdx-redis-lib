from hdx_redis_lib import connect_to_key_value_store_with_env_vars

KEY = 'redis-test-key'


def test_string_storage():
    test_string = 'ßräüëÉç_Üß€öä_úï'
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.set_string(KEY, test_string)
    retrieved_string = key_value_store.get_string(KEY)
    assert test_string == retrieved_string
