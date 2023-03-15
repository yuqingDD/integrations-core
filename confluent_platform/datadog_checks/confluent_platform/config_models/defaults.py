# (C) Datadog, Inc. 2021-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

# This file is autogenerated.
# To change this file you should edit assets/configuration/spec.yaml and then run the following commands:
#     ddev -x validate config -s <INTEGRATION_NAME>
#     ddev -x validate models -s <INTEGRATION_NAME>

from datadog_checks.base.utils.models.fields import get_default_field_value


def shared_collect_default_metrics(_field, _value):
    return False


def shared_conf(field, value):
    return get_default_field_value(field, value)


def shared_new_gc_metrics(_field, _value):
    return False


def shared_service(field, value):
    return get_default_field_value(field, value)


def instance_collect_default_jvm_metrics(_field, _value):
    return True


def instance_empty_default_hostname(_field, _value):
    return False


def instance_is_jmx(_field, _value):
    return False


def instance_java_bin_path(field, value):
    return get_default_field_value(field, value)


def instance_java_options(field, value):
    return get_default_field_value(field, value)


def instance_jmx_url(field, value):
    return get_default_field_value(field, value)


def instance_key_store_password(field, value):
    return get_default_field_value(field, value)


def instance_key_store_path(field, value):
    return get_default_field_value(field, value)


def instance_min_collection_interval(_field, _value):
    return 15


def instance_name(field, value):
    return get_default_field_value(field, value)


def instance_password(field, value):
    return get_default_field_value(field, value)


def instance_process_name_regex(field, value):
    return get_default_field_value(field, value)


def instance_rmi_client_timeout(_field, _value):
    return 15000


def instance_rmi_connection_timeout(_field, _value):
    return 20000


def instance_rmi_registry_ssl(_field, _value):
    return False


def instance_service(field, value):
    return get_default_field_value(field, value)


def instance_tags(field, value):
    return get_default_field_value(field, value)


def instance_tools_jar_path(field, value):
    return get_default_field_value(field, value)


def instance_trust_store_password(field, value):
    return get_default_field_value(field, value)


def instance_trust_store_path(field, value):
    return get_default_field_value(field, value)


def instance_user(field, value):
    return get_default_field_value(field, value)
