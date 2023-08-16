import pytest

from hdx_redis_lib import connect_to_key_value_store_with_env_vars


@pytest.fixture(scope='function', autouse=True)
def clean_redis() -> None:
    key_value_store = connect_to_key_value_store_with_env_vars()
    key_value_store.redis_conn.flushdb()