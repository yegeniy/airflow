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
"""EmrRunJobFlows manages cluster queue by implementing an EMR sensor."""

from airflow import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.utils.decorators import apply_defaults


class EmrRunJobFlows(EmrBaseSensor):
    """
    Submits batches of self-terminating EMR Job Flows and waits for their steps
    to complete. This operator submits a list of EMR clusters in batches, where
    each Job Flow is expected to be self-terminating and list all the EMR steps
    it is expected to execute. Includes basic retry logic.

    Note to future maintainer: The utility of the EmrBaseSensor that we extend
    is somewhat limited. Currently, it asks for the state of the JobFlow until
    that JobFlow reaches a terminal state. If the EMR JobFlow fails, the sensor
    will mark the task as failed. If custom EMR sensor logic implented, we could
    get step-wise monitoring and timeouts, get hooks for smart retries using
    XComs, and even implement cross-cluster logic, such as waiting for all
    clusters in a batch to finish - even when one fails.

    Implementation Note: For each cluster, we submit all the steps at cluster
    creation time. This partially frees the cluster from the vagaries of the
    Airflow scheduler. Since we'll rely on EMR to terminate itself, any failed
    step will need to terminate the cluster and the cluster itself should
    auto-terminate as per [1]. In other words, you must set up each JobFlow to
    auto-terminate (likely via its overrides) by setting
    `"KeepJobFlowAliveWhenNoSteps": False`. Also, consider setting each Step's
    `"ActionOnFailure": "TERMINATE_CLUSTER"` to allow failing-fast if your
    workflow allows for it.

    [1]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/\
    UsingEMR_TerminationProtection.html#emr-termination-protection-steps

    :param job_flows: jinja-templated str representing a queue of EMR JobFlows.
        A list of dicts, each one mapping job_flow names to their
        configurations: [{job_flow_name: job_flow_overrides}]. Each dict in the
        list represents the job flows which should run in parallel, and every
        cluster in the preceding dict is expected to have come to a successful
        terminal state, prior to submitting the next dict. (templated) boto3's
        job_flow_overrides EMR details are in https://boto3.amazonaws.com/v1/\
        documentation/api/latest/reference/services/emr.html\
        #EMR.Client.run_job_flow
    :type job_flows: str

    """

    template_fields = ['job_flows']
    template_ext = ()
    # EMR logo... ~RGB(237,165,83)
    ui_color = "#eda553"

    # Overrides for EmrBaseSensor
    NON_TERMINAL_STATES = EmrJobFlowSensor.NON_TERMINAL_STATES
    FAILED_STATE = EmrJobFlowSensor.FAILED_STATE

    @apply_defaults
    def __init__(
            self,
            job_flows,
            emr_conn_id='emr_default',
            # require_auto_termination = False,
            *args, **kwargs):
        """
        C0111
        """
        super().__init__(*args, **kwargs)
        self.job_flows = job_flows
        self.emr_conn_id = emr_conn_id
        # These two fields will be filled in as clusters are requested and poked
        self.current_batch = {}
        self.statuses = []

    def execute(self, context):
        """
        C0111
        """
        self.log.info(
            "The clusters will be submitted across the following batches: %s",
            [set(batch.keys()) for batch in self.job_flows])
        # TODO: Verify all clusters set `"KeepJobFlowAliveWhenNoSteps": False`
        # if self.require_auto_termination
        return super().execute(context)

    # override for EmrBaseSensor
    def get_emr_response(self):
        """
        C0111
        """
        emr_conn = EmrHook(emr_conn_id=self.emr_conn_id).get_conn()

        responses = []
        for name, job_flow_id in self.current_batch.items():
            self.log.debug("Poking JobFlow {%s: %s}", name, job_flow_id)
            response = emr_conn.describe_cluster(ClusterId=job_flow_id)
            responses.append(response)
            self.states()[name] = (job_flow_id, self._state_of(response))
        self.log.debug("Poked JobFlow states: %s", self.states())

        for failed in filter(lambda r: self._state_of(r) in
                             EmrRunJobFlows.FAILED_STATE, responses):
            self.log.info("there is at least one failed JobFlow")
            return failed
        for non_terminal in filter(lambda r: self._state_of(r) in
                                   EmrRunJobFlows.NON_TERMINAL_STATES,
                                   responses):
            self.log.info("there is still at least one non-terminal JobFlow")
            return non_terminal

        # We're done with the current batch.
        if self.job_flows:
            self.log.info("Submitting next batch of clusters")
            self.request_next(self.job_flows.pop(0))
            return self.get_emr_response()
        # All batches are in a terminal state
        else:
            self.log.info("Completed poking all JobFlow batches: %s",
                          self.statuses)
            return responses[0]

    def request_next(self, cluster_set):
        """
        C0111
        """
        self.current_batch = {}
        self.statuses.append({})
        errors = {}
        emr_hook = EmrHook(emr_conn_id=self.emr_conn_id)
        for name, cluster_config in cluster_set.items():
            self.log.info("[DEBUG] %s %s", name, cluster_config)
            response = emr_hook.create_job_flow(cluster_config)
            self.log.info("[DEBUG] %s", response)
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                errors[name] = str(response)
            else:
                job_flow_id = response["JobFlowId"]
                self.current_batch[name] = job_flow_id
                self.states()[name] = (job_flow_id, "")
        self.log.info("Requested JobFlow batch: %s", self.current_batch)

        # TODO consider returning {"statuses": statuses, "errors": errors}
        if errors:
            self.log.error("errors: %s", errors)
            raise AirflowException('JobFlow creation failed: ' + errors)

    def states(self):
        """
        C0111
        """
        return self.statuses[-1] if self.statuses else {}

    @staticmethod
    def _state_of(response):
        """
        C0111
        """
        return response.get("Cluster", {}).get("Status", {}).get("State", "")

    # override for EmrBaseSensor. Not using _state_of(), since
    # state_from_response expects an exception raised if the cluster State is
    # not present.
    @staticmethod
    def state_from_response(response):
        """
        C0111
        """
        return EmrJobFlowSensor.state_from_response(response)

    @staticmethod
    def failure_message_from_response(response):
        """
        C0111
        """
        return EmrJobFlowSensor.failure_message_from_response(response)
