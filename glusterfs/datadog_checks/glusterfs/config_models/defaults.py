# (C) Datadog, Inc. 2021-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

# This file is autogenerated.
# To change this file you should edit assets/configuration/spec.yaml and then run the following commands:
#     ddev -x validate config -s <INTEGRATION_NAME>
#     ddev -x validate models -s <INTEGRATION_NAME>

from datadog_checks.base.utils.models.fields import get_default_field_value


def shared_gstatus_path(_field, _value):
    return '/opt/datadog-agent/embedded/sbin/gstatus'


def shared_service(field, value):
    return get_default_field_value(field, value)


def instance_disable_generic_tags(_field, _value):
    return False


def instance_empty_default_hostname(_field, _value):
    return False


def instance_metric_patterns(field, value):
    return get_default_field_value(field, value)


def instance_min_collection_interval(_field, _value):
    return 60


def instance_service(field, value):
    return get_default_field_value(field, value)


def instance_tags(field, value):
    return get_default_field_value(field, value)


def instance_use_sudo(_field, _value):
    return True
