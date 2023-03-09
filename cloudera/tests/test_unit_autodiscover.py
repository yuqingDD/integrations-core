# (C) Datadog, Inc. 2023-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

from contextlib import contextmanager
from contextlib import nullcontext as does_not_raise

import mock
import pytest
from packaging.version import Version
from tests.common import query_time_series

from datadog_checks.base.types import ServiceCheck
from datadog_checks.cloudera.metrics import NATIVE_METRICS, TIMESERIES_METRICS

pytestmark = [pytest.mark.unit]


def make_host(**overrides):
    host = {
        'host_id': 'ABCDEFG01',
        'name': 'host_0',
        'entity_status': 'GOOD_HEALTH',
        'num_cores': 8,
        'num_physical_cores': 8,
        'total_phys_mem_bytes': 33079799808,
        'rack_id': 'rack_id_0',
        'tags': [],
    }

    host.update(overrides)
    return host


@pytest.mark.parametrize(
    'instance, read_clusters, expected_exception, expected_can_connects, expected_cluster_healths, expected_metrics',
    [
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'include': {'^cluster.*'}}},
            [],
            pytest.raises(
                Exception,
                match='Setting `include` must be an array',
            ),
            [
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'message': 'Cloudera API Client is none: Setting `include` must be an array',
                    'tags': ['api_url:http://localhost:8080/api/v48/'],
                }
            ],
            [{'count': 0}],
            [{'count': 0}],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'tags': ['new_tag'], 'clusters': {'include': {'^cluster.*'}}},
            [],
            pytest.raises(
                Exception,
                match='Setting `include` must be an array',
            ),
            [
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'message': 'Cloudera API Client is none: Setting `include` must be an array',
                    'tags': ['api_url:http://localhost:8080/api/v48/', 'new_tag'],
                }
            ],
            [{'count': 0}],
            [{'count': 0}],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'include': [[]]}},
            [],
            pytest.raises(
                Exception,
                match='`include` entries must be a map or a string',
            ),
            [
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'message': 'Cloudera API Client is none: `include` entries must be a map or a string',
                    'tags': ['api_url:http://localhost:8080/api/v48/'],
                }
            ],
            [{'count': 0}],
            [{'count': 0}],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'tags': ['new_tag'], 'clusters': {'include': [[]]}},
            [],
            pytest.raises(
                Exception,
                match='`include` entries must be a map or a string',
            ),
            [
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'message': 'Cloudera API Client is none: `include` entries must be a map or a string',
                    'tags': ['api_url:http://localhost:8080/api/v48/', 'new_tag'],
                }
            ],
            [{'count': 0}],
            [{'count': 0}],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'include': ['^cluster.*']}},
            [],
            does_not_raise(),
            [{'count': 1, 'status': ServiceCheck.OK, 'tags': ['api_url:http://localhost:8080/api/v48/']}],
            [{'count': 0}],
            [{'count': 0}],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'include': ['^cluster.*']}},
            [
                {'name': 'cluster_0', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_new_0', 'entity_status': 'GOOD_HEALTH'},
            ],
            does_not_raise(),
            [{'count': 1, 'status': ServiceCheck.OK, 'tags': ['api_url:http://localhost:8080/api/v48/']}],
            [
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_new_0']},
            ],
            [
                {'count': 1, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 1, 'tags': ['cloudera_cluster:cluster_new_0']},
            ],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'limit': 5, 'include': ['^cluster.*']}},
            [
                {'name': 'cluster_0', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_1', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_2', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_3', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_4', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_5', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_6', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_7', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_8', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'cluster_9', 'entity_status': 'GOOD_HEALTH'},
            ],
            does_not_raise(),
            [{'count': 1, 'status': ServiceCheck.OK, 'tags': ['api_url:http://localhost:8080/api/v48/']}],
            [
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_1']},
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_2']},
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_3']},
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_4']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_5']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_6']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_7']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_8']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_9']},
            ],
            [
                {'count': 1, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 1, 'tags': ['cloudera_cluster:cluster_1']},
                {'count': 1, 'tags': ['cloudera_cluster:cluster_2']},
                {'count': 1, 'tags': ['cloudera_cluster:cluster_3']},
                {'count': 1, 'tags': ['cloudera_cluster:cluster_4']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_5']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_6']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_7']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_8']},
                {'count': 0, 'tags': ['cloudera_cluster:cluster_9']},
            ],
        ),
        (
            {'api_url': 'http://localhost:8080/api/v48/', 'clusters': {'include': ['.*'], 'exclude': ['^tmp_.*']}},
            [
                {'name': 'cluster_0', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'tmp_0', 'entity_status': 'GOOD_HEALTH'},
            ],
            does_not_raise(),
            [{'count': 1, 'status': ServiceCheck.OK, 'tags': ['api_url:http://localhost:8080/api/v48/']}],
            [
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 0, 'tags': ['cloudera_cluster:tmp_0']},
            ],
            [
                {'count': 1, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 0, 'tags': ['cloudera_cluster:tmp_0']},
            ],
        ),
        (
            {
                'api_url': 'http://localhost:8080/api/v48/',
                'clusters': {'include': [{'.*': {}}], 'exclude': ['^tmp_.*']},
            },
            [
                {'name': 'cluster_0', 'entity_status': 'GOOD_HEALTH'},
                {'name': 'tmp_0', 'entity_status': 'GOOD_HEALTH'},
            ],
            does_not_raise(),
            [{'count': 1, 'status': ServiceCheck.OK, 'tags': ['api_url:http://localhost:8080/api/v48/']}],
            [
                {'count': 1, 'status': ServiceCheck.OK, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 0, 'tags': ['cloudera_cluster:tmp_0']},
            ],
            [
                {'count': 1, 'tags': ['cloudera_cluster:cluster_0']},
                {'count': 0, 'tags': ['cloudera_cluster:tmp_0']},
            ],
        ),
    ],
    ids=[
        'exception include type',
        'exception include type with custom tags',
        'exception include entry type',
        'exception include entry type with custom tags',
        'configured cluster autodiscover with zero clusters',
        'configured cluster autodiscover with two different prefix clusters',
        'configured cluster autodiscover with ten clusters and limit',
        'configured cluster autodiscover with two different prefix clusters and one of them excluded',
        'configured cluster autodiscover (with dict) with two different prefix clusters and one of them excluded',
    ],
)
def test_autodiscover_clusters(
    aggregator,
    dd_run_check,
    cloudera_check,
    instance,
    read_clusters,
    expected_exception,
    expected_can_connects,
    expected_cluster_healths,
    expected_metrics,
):
    with expected_exception, mock.patch(
        'datadog_checks.cloudera.client.cm_client.CmClient.get_version',
        return_value=Version('7.0.0'),
    ), mock.patch(
        'datadog_checks.cloudera.client.cm_client.CmClient.read_clusters',
        return_value=read_clusters,
    ), mock.patch(
        'datadog_checks.cloudera.client.cm_client.CmClient.query_time_series',
        side_effect=query_time_series,
    ), mock.patch(
        'datadog_checks.cloudera.client.cm_client.CmClient.list_hosts',
        return_value=[],
    ), mock.patch(
        'datadog_checks.cloudera.client.cm_client.CmClient.read_events',
        return_value=[],
    ):
        check = cloudera_check(instance)
        dd_run_check(check)
        for expected_can_connect in expected_can_connects:
            aggregator.assert_service_check(
                'cloudera.can_connect',
                count=expected_can_connect.get('count'),
                status=expected_can_connect.get('status'),
                message=expected_can_connect.get('message'),
                tags=expected_can_connect.get('tags'),
            )
        for expected_cluster_health in expected_cluster_healths:
            aggregator.assert_service_check(
                'cloudera.cluster.health',
                count=expected_cluster_health.get('count'),
                status=expected_cluster_health.get('status'),
                message=expected_cluster_health.get('message'),
                tags=expected_cluster_health.get('tags'),
            )
        for expected_metric in expected_metrics:
            for metric in TIMESERIES_METRICS['cluster']:
                aggregator.assert_metric(
                    f'cloudera.cluster.{metric}', count=expected_metric.get('count'), tags=expected_metric.get('tags')
                )


@pytest.mark.parametrize(
    'extra_tags', [pytest.param([], id='no extra tags'), pytest.param(['new_tag'], id='with custom tags')]
)
@pytest.mark.parametrize(
    'include, expected_error_message',
    [
        pytest.param(
            {'^host.*'},
            'Cloudera check raised an exception: Setting `include` must be an array',
            id='bad type for include',
        ),
        pytest.param(
            [[]],
            'Cloudera check raised an exception: `include` entries must be a map or a string',
            id='bad type for include entry',
        ),
    ],
)
def test_autodiscover_hosts_with_include_config_errors(
    aggregator, dd_run_check, cloudera_check, include, expected_error_message, extra_tags
):
    instance = {
        'api_url': 'http://localhost:8080/api/v48/',
        'clusters': {
            'include': [
                {
                    '^cluster.*': {
                        'hosts': {
                            'include': include,
                        }
                    }
                }
            ]
        },
    }

    if extra_tags:
        instance['tags'] = extra_tags

    with patch_cm_client():
        check = cloudera_check(instance)
        dd_run_check(check)

    aggregator.assert_service_check(
        'cloudera.can_connect',
        count=1,
        status=ServiceCheck.CRITICAL,
        message=expected_error_message,
        tags=['api_url:http://localhost:8080/api/v48/'] + extra_tags,
    )
    # Since this is a config error, we shouldn't report host health related checks
    aggregator.assert_service_check('cloudera.host.health', count=0)
    # Likewise, we shouldn't get any host-related metrics
    assert len([name for name in aggregator.metric_names if name.startswith('cloudera.host.')]) == 0


def test_autodiscover_hosts_with_zero_hosts(aggregator, dd_run_check, cloudera_check):
    instance = {
        'api_url': 'http://localhost:8080/api/v48/',
        'clusters': {
            'include': [
                {
                    '^cluster.*': {
                        'hosts': {
                            'include': ['^host.*'],
                        }
                    }
                }
            ]
        },
    }

    with patch_cm_client() as mock_client:
        check = cloudera_check(instance)
        dd_run_check(check)

    assert mock_client.list_hosts.call_count == 1

    aggregator.assert_service_check(
        'cloudera.can_connect',
        count=1,
        status=ServiceCheck.OK,
        tags=['api_url:http://localhost:8080/api/v48/'],
    )

    # We shouldn't get any host-related metrics as there are no hosts
    assert len([name for name in aggregator.metric_names if name.startswith('cloudera.host.')]) == 0


@pytest.mark.parametrize(
    'hosts_conf, dd_run_check_count, list_hosts, expected_host_healths, expected_metrics, list_hosts_call_count',
    [
        (
            {
                'include': ['^host.*'],
            },
            1,
            [
                make_host(name='host_0', entity_status='BAD_HEALTH'),
                make_host(name='host_new_0', entity_status='BAD_HEALTH'),
            ],
            [
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                },
                {
                    'count': 1,
                    'status': ServiceCheck.CRITICAL,
                    'tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_new_0',
                        'cloudera_rack_id:rack_id_0',
                    ],
                },
            ],
            [
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
                {
                    'count': 1,
                    'tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_new_0',
                        'cloudera_rack_id:rack_id_0',
                    ],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_new_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_new_0',
                    ],
                },
            ],
            1,
        ),
        (
            {
                'limit': 5,
                'include': ['^host.*'],
            },
            1,
            [make_host(name=f'host_{n}') for n in range(10)],
            [
                # The first 5 hosts report service checks
                {
                    'count': 1,
                    'status': ServiceCheck.OK,
                    'tags': ['cloudera_cluster:cluster_0', f'cloudera_hostname:host_{n}', 'cloudera_rack_id:rack_id_0'],
                }
                for n in range(5)
            ]
            + [
                # And the rest do not (due to the limit in the config)
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', f'cloudera_hostname:host_{n}', 'cloudera_rack_id:rack_id_0'],
                }
                for n in range(5, 10)
            ],
            [
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_1', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_1',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_1',
                    ],
                },
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_2', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_2',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_2',
                    ],
                },
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_3', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_3',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_3',
                    ],
                },
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_4', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_4',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_4',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_5', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_5',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_5',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_6', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_6',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_6',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_7', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_7',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_7',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_8', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_8',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_8',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_9', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_9',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_9',
                    ],
                },
            ],
            1,
        ),
        (
            {
                'include': ['.*'],
                'exclude': ['^tmp_.*'],
            },
            1,
            [
                make_host(name='host_0'),
                make_host(name='tmp_0'),
            ],
            [
                {
                    'count': 1,
                    'status': ServiceCheck.OK,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:tmp_0', 'cloudera_rack_id:rack_id_0'],
                },
            ],
            [
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:tmp_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:tmp_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:tmp_0',
                    ],
                },
            ],
            1,
        ),
        (
            {
                'include': [{'.*': {}}],
                'exclude': ['^tmp_.*'],
            },
            1,
            [
                make_host(name='host_0'),
                make_host(name='tmp_0'),
            ],
            [
                {
                    'count': 1,
                    'status': ServiceCheck.OK,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:tmp_0', 'cloudera_rack_id:rack_id_0'],
                },
            ],
            [
                {
                    'count': 1,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
                {
                    'count': 0,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:tmp_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:tmp_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:tmp_0',
                    ],
                },
            ],
            1,
        ),
        (
            {
                'include': ['^host.*'],
            },
            2,
            [make_host(name='host_0', entity_status='BAD_HEALTH')],
            [
                {
                    'count': 2,
                    'status': ServiceCheck.CRITICAL,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                },
            ],
            [
                {
                    'count': 2,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
            ],
            2,
        ),
        (
            {
                'interval': 60,
                'include': ['^host.*'],
            },
            2,
            [
                make_host(entity_status='BAD_HEALTH'),
            ],
            [
                {
                    'count': 2,
                    'status': ServiceCheck.CRITICAL,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                },
            ],
            [
                {
                    'count': 2,
                    'tags': ['cloudera_cluster:cluster_0', 'cloudera_hostname:host_0', 'cloudera_rack_id:rack_id_0'],
                    'ts_tags': [
                        'cloudera_cluster:cluster_0',
                        'cloudera_hostname:host_0',
                        'cloudera_rack_id:rack_id_0',
                        'cloudera_host:host_0',
                    ],
                },
            ],
            1,
        ),
    ],
    ids=[
        'configured host autodiscover with two different prefix hosts',
        'configured host autodiscover with ten clusters and limit',
        'configured host autodiscover with two different prefix hosts and one of them excluded',
        'configured host autodiscover (with dict) with two different prefix hosts and one of them excluded',
        'configured host autodiscover without interval with one host when run check two times then two remote calls',
        'configured host autodiscover with interval with one host when run check two times then only one remote call',
    ],
)
def test_autodiscover_hosts(
    aggregator,
    dd_run_check,
    cloudera_check,
    hosts_conf,
    dd_run_check_count,
    list_hosts,
    expected_host_healths,
    expected_metrics,
    list_hosts_call_count,
):

    instance = {
        'api_url': 'http://localhost:8080/api/v48/',
        'clusters': {
            'include': [
                {
                    '^cluster.*': {
                        'hosts': hosts_conf,
                    }
                }
            ]
        },
    }

    with patch_cm_client(list_hosts=list_hosts) as mock_client:
        check = cloudera_check(instance)
        for _ in range(dd_run_check_count):
            dd_run_check(check)

        aggregator.assert_service_check(
            'cloudera.can_connect',
            count=dd_run_check_count,
            status=ServiceCheck.OK,
            tags=['api_url:http://localhost:8080/api/v48/'],
        )
        for expected_host_health in expected_host_healths:
            aggregator.assert_service_check(
                'cloudera.host.health',
                count=expected_host_health.get('count'),
                status=expected_host_health.get('status'),
                message=expected_host_health.get('message'),
                tags=expected_host_health.get('tags'),
            )
        for expected_metric in expected_metrics:
            for metric in NATIVE_METRICS['host']:
                aggregator.assert_metric(
                    f'cloudera.host.{metric}', count=expected_metric.get('count'), tags=expected_metric.get('tags')
                )
            for metric in TIMESERIES_METRICS['host']:
                aggregator.assert_metric(
                    f'cloudera.host.{metric}', count=expected_metric.get('count'), tags=expected_metric.get('ts_tags')
                )
        assert mock_client.list_hosts.call_count == list_hosts_call_count


@contextmanager
def patch_cm_client(*, read_clusters=None, list_hosts=None):
    """Patch CMClient with default return values with some customization"""
    if read_clusters is None:
        read_clusters = [
            {'name': 'cluster_0', 'entity_status': 'GOOD_HEALTH'},
        ]
    if list_hosts is None:
        list_hosts = []

    with mock.patch(
        'datadog_checks.cloudera.client.factory.CmClient',
        autospec=True,
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.get_version.return_value = Version('7.0.0')
        mock_client.read_clusters.return_value = read_clusters
        mock_client.query_time_series.side_effect = query_time_series
        mock_client.list_hosts.return_value = list_hosts

        yield mock_client
