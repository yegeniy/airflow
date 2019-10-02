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

import unittest
from unittest.mock import MagicMock, patch
import datetime
from dateutil.tz import tzlocal

from airflow import AirflowException
from airflow.contrib.sensors.emr_run_job_flows import EmrRunJobFlows

class TestEmrRunJobFlows(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()
        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

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
        calls = [
            # First, cluster1 is queried until it terminates
            _describe_cluster("cluster1", "STARTING"),
            _describe_cluster("cluster1", "BOOTSTRAPPING"),
            _describe_cluster("cluster1", "RUNNING"),
            _describe_cluster("cluster1", "RUNNING"),
            _describe_cluster("cluster1", "TERMINATING"),
            _describe_cluster("cluster1", "TERMINATED"),      # (end of batch)
            # Then, both cluster2a and cluster2b are queried
            _describe_cluster("cluster2a", "STARTING"),       # a
            _describe_cluster("cluster2b", "STARTING"),       # b
            _describe_cluster("cluster2a", "BOOTSTRAPPING"),  # a
            _describe_cluster("cluster2b", "BOOTSTRAPPING"),  # b
            _describe_cluster("cluster2a", "RUNNING"),        # a
            _describe_cluster("cluster2b", "RUNNING"),        # b
            _describe_cluster("cluster2a", "RUNNING"),        # a
            _describe_cluster("cluster2b", "RUNNING"),        # b
            _describe_cluster("cluster2a", "RUNNING"),        # a
            _describe_cluster("cluster2b", "TERMINATING"),    # b
            _describe_cluster("cluster2a", "RUNNING"),        # a
            _describe_cluster("cluster2b", "TERMINATED"),     # b (finished)
            _describe_cluster("cluster2a", "RUNNING"),        # a
            _describe_cluster("cluster2b", "TERMINATING"),    # a
            _describe_cluster("cluster2b", "TERMINATED"),     # b (finished)
            _describe_cluster("cluster2b", "TERMINATED"),     # a (end of batch)
            _describe_cluster("cluster2b", "TERMINATED"),     # b (end of batch)
            # Finally, cluster3 is queried until it terminates
            _describe_cluster("clusters3", "STARTING"),
            _describe_cluster("clusters3", "BOOTSTRAPPING"),
            _describe_cluster("clusters3", "RUNNING"),
            _describe_cluster("clusters3", "RUNNING"),
            _describe_cluster("clusters3", "TERMINATING"),
            _describe_cluster("clusters3", "TERMINATED"),     # (all finished)
        ]
        self.emr_client_mock.describe_cluster.side_effect = calls
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.emr_run_job_flows.execute(None)
            self._assertWeMade(len(calls))

    def test_execute_fails_fast_when_cluster2b_fails(self):
        calls = [
            # First, cluster1 is queried until it terminates
            _describe_cluster("cluster1", "STARTING"),
            _describe_cluster("cluster1", "BOOTSTRAPPING"),
            _describe_cluster("cluster1", "RUNNING"),
            _describe_cluster("cluster1", "RUNNING"),
            _describe_cluster("cluster1", "TERMINATING"),
            _describe_cluster("cluster1", "TERMINATED"),
            # Then, both cluster2a and cluster2b are queried
            _describe_cluster("cluster2a", "STARTING"),                # a
            _describe_cluster("cluster2b", "STARTING"),                # b
            _describe_cluster("cluster2a", "BOOTSTRAPPING"),           # a
            _describe_cluster("cluster2b", "BOOTSTRAPPING"),           # b
            _describe_cluster("cluster2a", "RUNNING"),                 # a
            _describe_cluster("cluster2b", "RUNNING"),                 # b
            _describe_cluster("cluster2a", "RUNNING"),                 # a
            _describe_cluster("cluster2b", "RUNNING"),                 # b
            _describe_cluster("cluster2a", "TERMINATING"),             # a
            _describe_cluster("cluster2b", "TERMINATED_WITH_ERRORS"),  # b 
            # We expect that no more calls are to be made, even though cluster3
            # hasn't even started and cluster2a isn't yet terminated.
        ]
        self.emr_client_mock.describe_cluster.side_effect = calls
        with patch('boto3.session.Session', self.boto3_session_mock):
            with self.assertRaises(AirflowException):
                self.emr_run_job_flows.execute(None)

                self._assertWeMade(len(calls))

    def _assertWeMade(self, n_calls):
        self.assertEqual(
            self.emr_client_mock.describe_cluster.call_count,
            len(calls))

# Convenience methods for describing clusters
def running_cluster(self, name, state="RUNNING"):
    return {
        'Cluster': {
            'Applications': [
                {'Name': 'Spark', 'Version': '1.6.1'}
            ],
            'AutoTerminate': True,
            'Configurations': [],
            'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
            'Id': f'j-{name}',
            'LogUri': 's3n://some-location/',
            'Name': 'PiCalc',
            'NormalizedInstanceHours': 0,
            'ReleaseLabel': 'emr-4.6.0',
            'ServiceRole': 'EMR_DefaultRole',
            'Status': {
                'State': state,
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
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


def terminated_cluster(self, name):
    return {
        'Cluster': {
            'Applications': [
                {'Name': 'Spark', 'Version': '1.6.1'}
            ],
            'AutoTerminate': True,
            'Configurations': [],
            'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
            'Id': f'j-{name}',
            'LogUri': 's3n://some-location/',
            'Name': 'PiCalc',
            'NormalizedInstanceHours': 0,
            'ReleaseLabel': 'emr-4.6.0',
            'ServiceRole': 'EMR_DefaultRole',
            'Status': {
                'State': 'TERMINATED',
                'StateChangeReason': {},
                'Timeline': {
                    'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
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


def failed_cluster(self, name):
    return {
        'Cluster': {
            'Applications': [
                {'Name': 'Spark', 'Version': '1.6.1'}
            ],
            'AutoTerminate': True,
            'Configurations': [],
            'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
            'Id': f'j-{name}',
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
                    'CreationDateTime': datetime.datetime(2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
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


def _describe_cluster(named, state)
    return {
        'TERMINATED': terminated_cluster(named),
        'TERMINATED_WITH_ERRORS': failed_cluster(named),
    }.get(state, running_cluster(named, state))


if __name__ == '__main__':
    unittest.main()
