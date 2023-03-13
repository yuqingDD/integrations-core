# (C) Datadog, Inc. 2023-present
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

import re

from datadog_checks.base import is_affirmative

from .exceptions import IncompleteConfig
from .settings import DEFAULT_PAGINATED_LIMIT


class OpenstackConfig(object):
    def __init__(self, instance, log):
        self.log = log

        self.instance_name = instance.get('name')
        self.network_ids = instance.get('network_ids', [])
        self.exclude_network_id_patterns = set(instance.get('exclude_network_ids', []))
        self.exclude_network_id_rules = [re.compile(ex) for ex in self.exclude_network_id_patterns]
        self.exclude_server_id_patterns = set(instance.get('exclude_server_ids', []))
        self.exclude_server_id_rules = [re.compile(ex) for ex in self.exclude_server_id_patterns]
        self.include_project_name_patterns = set(instance.get('whitelist_project_names', []))
        self.include_project_name_rules = [re.compile(ex) for ex in self.include_project_name_patterns]
        self.exclude_project_name_patterns = set(instance.get('blacklist_project_names', []))
        self.exclude_project_name_rules = [re.compile(ex) for ex in self.exclude_project_name_patterns]

        self.custom_tags = instance.get("tags", [])
        self.collect_project_metrics = is_affirmative(instance.get('collect_project_metrics', True))
        self.collect_hypervisor_metrics = is_affirmative(instance.get('collect_hypervisor_metrics', True))
        self.collect_hypervisor_load = is_affirmative(instance.get('collect_hypervisor_load', True))
        self.collect_network_metrics = is_affirmative(instance.get('collect_network_metrics', True))
        self.collect_server_diagnostic_metrics = is_affirmative(instance.get('collect_server_diagnostic_metrics', True))
        self.collect_server_flavor_metrics = is_affirmative(instance.get('collect_server_flavor_metrics', True))
        self.use_shortname = is_affirmative(instance.get('use_shortname', False))

        self.user = instance.get('user', None)

        self.paginated_limit = instance.get('paginated_limit', DEFAULT_PAGINATED_LIMIT)
        self.openstack_config_file_path = instance.get("openstack_config_file_path")
        self.openstack_cloud_name = instance.get("openstack_cloud_name")
        self.keystone_server_url = instance.get("keystone_server_url")
        self.validate_config()

    def validate_config(self):

        # We need a instance_name to identify this instance
        if not self.instance_name:
            raise IncompleteConfig("Missing name")

        """
        Parse user identity out of config

        To guarantee a uniquely identifiable user, expects
        {"user": {"name": "my_username", "password": "my_password",
                  "domain": {"id": "my_domain_id"}
                  }
        }
        """
        if self.openstack_cloud_name is None:
            if not (
                self.user
                and self.user.get('name')
                and self.user.get('password')
                and self.user.get("domain")
                and self.user.get("domain").get("id")
            ):
                self.log.warning(
                    "Please specify the user via the `user` variable in your openstack_controller configuration.\n"
                    "This is the user you would use to authenticate with Keystone v3 via password auth.\n"
                    "The user should look like: "
                    "{'password': 'my_password', 'name': 'my_name', 'domain': {'id': 'my_domain_id'}}"
                )
