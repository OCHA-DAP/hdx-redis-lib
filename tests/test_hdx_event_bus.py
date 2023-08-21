from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars
from hdx_redis_lib.redis_connections import connect_to_hdx_write_only_event_bus_with_env_vars


def _generate_test_event():
    return {
        "event_type": "dataset-metadata-changed",
        "event_time": "2023-08-02T20:40:49.402615",
        "event_source": "ckan",
        "initiator_user_name": "test-user",
        "dataset_name": "test-dataset",
        "dataset_title": "Test dataset",
        "dataset_id": "test-dataset-id",
        "changed_fields": [
            {
                "field": "notes",
                "new_value": "New description",
                "new_display_value": "New description",
                "old_value": "Old description",
                "old_display_value": "Old description",
            },
            {
                "field": "tags",
                "added_items": [],
                "removed_items": ["removed-test-tag"],
            },
        ],
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def test_read_write_hdx_event_bus():
    write_event_bus = connect_to_hdx_write_only_event_bus_with_env_vars()
    event = _generate_test_event()
    write_event_bus.push_hdx_event(event)

    event_bus = connect_to_hdx_event_bus_with_env_vars()
    retrieved_event = event_bus.read_hdx_event()
    assert retrieved_event == event
