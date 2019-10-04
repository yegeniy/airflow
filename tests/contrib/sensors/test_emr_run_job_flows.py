# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import unittest
from unittest.mock import MagicMock, patch

from dateutil.tz import tzlocal

from airflow import DAG, AirflowException
from airflow.contrib.sensors.emr_run_job_flows import EmrRunJobFlows
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestEmrRunJobFlows(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()
        self.boto3_session_mock = None  # set up in _expect
        self.emr_run_job_flows = EmrRunJobFlows(
            task_id='test_task',
            job_flows=self._stubbed_job_flows([
                ["cluster1"],                # first batch
                ["cluster2a", "cluster2b"],  # then these two run in parallel
                ["cluster3"]]),              # and finally, this third batch
            dag=DAG('test_dag_id', default_args=args)
        )

    def _stubbed_job_flows(self, names_queue):
        job_flows = []
        for names_batch in names_queue:
            job_flows_batch = {}
            for name in names_batch:
                job_flows_batch[name] = self._cluster_config(name)
            job_flows.append(job_flows_batch)
        return job_flows

    def _cluster_config(self, name):
        return {
            'Name': name,
            'ReleaseLabel': '5.11.0',
            'Steps': [{
                'Name': 'test_step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        '/usr/lib/spark/bin/run-example',
                        '{{ macros.ds_add(ds, -1) }}',
                        '{{ ds }}'
                    ]
                }
            }]
        }

    def test_execute_calls_until_all_clusters_reach_a_terminal_state(self):
        create_calls = [
            self._create("cluster1"),
            self._create("cluster2a"),
            self._create("cluster2b"),
            self._create("cluster3"),
        ]
        describe_calls = [
            # First, cluster1 then queried until it terminates
            self._describe("cluster1", "STARTING"),
            self._describe("cluster1", "BOOTSTRAPPING"),
            self._describe("cluster1", "RUNNING"),
            self._describe("cluster1", "RUNNING"),
            self._describe("cluster1", "TERMINATING"),
            self._describe("cluster1", "TERMINATED"),      # (EOBatch)
            # Then, both cluster2a and cluster2b are queried
            self._describe("cluster2a", "STARTING"),       # a
            self._describe("cluster2b", "STARTING"),       # b
            self._describe("cluster2a", "BOOTSTRAPPING"),  # a
            self._describe("cluster2b", "BOOTSTRAPPING"),  # b
            self._describe("cluster2a", "RUNNING"),        # a
            self._describe("cluster2b", "RUNNING"),        # b
            self._describe("cluster2a", "RUNNING"),        # a
            self._describe("cluster2b", "RUNNING"),        # b
            self._describe("cluster2a", "RUNNING"),        # a
            self._describe("cluster2b", "TERMINATING"),    # b
            self._describe("cluster2a", "RUNNING"),        # a
            self._describe("cluster2b", "TERMINATED"),     # b: terminal
            self._describe("cluster2a", "RUNNING"),        # a
            self._describe("cluster2b", "TERMINATING"),    # a
            self._describe("cluster2b", "TERMINATED"),     # b: terminal
            self._describe("cluster2b", "TERMINATED"),     # a (EOBatch)
            self._describe("cluster2b", "TERMINATED"),     # b (EOBatch)
            # Finally, cluster3 then queried until it terminates
            self._describe("clusters3", "STARTING"),
            self._describe("clusters3", "BOOTSTRAPPING"),
            self._describe("clusters3", "RUNNING"),
            self._describe("clusters3", "RUNNING"),
            self._describe("clusters3", "TERMINATING"),
            self._describe("clusters3", "TERMINATED"),     # (all done)
        ]

        self._expect(create_calls, describe_calls)

    def test_execute_fails_fast_when_cluster2b_fails(self):
        create_calls = [
            self._create("cluster1"),
        ]
        describe_calls = [
            # First, cluster1 is queried until it terminates
            self._describe("cluster1", "STARTING"),
            self._describe("cluster1", "BOOTSTRAPPING"),
            self._describe("cluster1", "RUNNING"),
            self._describe("cluster1", "RUNNING"),
            self._describe("cluster1", "TERMINATING"),
            self._describe("cluster1", "TERMINATED"),
            # Then, both cluster2a and cluster2b are queried
            self._describe("cluster2a", "STARTING"),                # a
            self._describe("cluster2b", "STARTING"),                # b
            self._describe("cluster2a", "BOOTSTRAPPING"),           # a
            self._describe("cluster2b", "BOOTSTRAPPING"),           # b
            self._describe("cluster2a", "RUNNING"),                 # a
            self._describe("cluster2b", "RUNNING"),                 # b
            self._describe("cluster2a", "RUNNING"),                 # a
            self._describe("cluster2b", "RUNNING"),                 # b
            self._describe("cluster2a", "TERMINATING"),             # a
            self._describe("cluster2b", "TERMINATED_WITH_ERRORS"),  # b
            # We expect that no more calls are to be made, even though cluster3
            # hasn't even started and cluster2a isn't yet terminated.
        ]

        self._expect(create_calls, describe_calls, failure=True)

    def _expect(self, create_calls, describe_calls, failure=False):
        self.emr_client_mock.describe_cluster.side_effect = self._describe
        self.emr_client_mock.run_job_flow.side_effect = self._create

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)
        with patch('boto3.session.Session', self.boto3_session_mock):
            try:
                if failure:
                    with self.assertRaises(AirflowException):
                        self._verify_call_counts(create_calls, describe_calls)
                else:
                    self._verify_call_counts(create_calls, describe_calls)
            except Exception as e:
                raise e

    def _verify_call_counts(self, create_calls, describe_calls):
        self.emr_run_job_flows.execute(None)
        self._assert_we_made(self.emr_client_mock.run_job_flow, create_calls)
        self._assert_we_made(self.emr_client_mock.describe_cluster,
                             describe_calls)

    # def test_execute_fails_fast_on_cluster_creation(self):
        # TODO: assertRaises(AirflowException):...

    def _assert_we_made(self, mock, calls):
        self.assertEqual(mock.call_count, len(calls))

    # Convenience methods for describing clusters
    def _running_cluster(self, name, state="RUNNING"):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': 'j-' + name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': state,
                    'StateChangeReason': {},
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _terminated_cluster(self, name):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': 'j-' + name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': 'TERMINATED',
                    'StateChangeReason': {},
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _failed_cluster(self, name):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': 'j-' + name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': 'TERMINATED_WITH_ERRORS',
                    'StateChangeReason': {
                        'Code': 'BOOTSTRAP_FAILURE',
                        'Message': 'Master instance (i-0663047709b12345c) failed attempting to '
                                   'download bootstrap action 1 file from S3'
                    },
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _describe(self, named, state):
        return {
            'TERMINATED': self._terminated_cluster(named),
            'TERMINATED_WITH_ERRORS': self._failed_cluster(named),
        }.get(state, self._running_cluster(named, state))

    def _create(self, config_or_name):
        print("[DEBUG] _create(%s: %s)", config_or_name, type(config_or_name))
        if isinstance(config_or_name, str):
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                },
                'JobFlowId': 'j-' + config_or_name
            }
        else:
            return self._create(config_or_name['Name'])


if __name__ == '__main__':
    unittest.main()
