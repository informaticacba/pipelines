# Copyright 2021 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""GCP launcher for Dataproc Batch workloads."""

import json
import logging
import os
from os import path
import re
import time
from typing import Any, Dict, Optional

import google.auth.transport.requests
from google_cloud_pipeline_components.proto import gcp_resources_pb2
import requests
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util.retry import Retry
from .utils import json_util

from google.protobuf import json_format


_POLL_INTERVAL_SECONDS = 20
_CONNECTION_ERROR_RETRY_LIMIT = 5
_CONNECTION_RETRY_BACKOFF_FACTOR = 1.

_DATAPROC_BATCH_NAME_PREFIX = 'https://dataproc.googleapis.com/v1'
_DATAPROC_BATCH_NAME_TEMPLATE = rf'({_DATAPROC_BATCH_NAME_PREFIX}/projects/(?P<project>.*)/locations/(?P<location>.*)/batches/(?P<batch>.*))'


class DataprocBatchRemoteRunner():
  """Common module for creating and polling Dataproc Serverless Batch workloads."""

  def __init__(
      self,
      type: str,
      project: str,
      location: str,
      gcp_resources: str
  ) -> None:
    """Initializes a DataprocBatchRemoteRunner object."""
    self._type = type
    self._project = project
    self._location = location
    self._creds, _ = google.auth.default()
    self._gcp_resources = gcp_resources
    self._session = self._get_requests_session()

  def _get_requests_session(self) -> Session:
    """Gets a http session."""
    retry = Retry(
        total=_CONNECTION_ERROR_RETRY_LIMIT,
        status_forcelist=[503],
        backoff_factor=_CONNECTION_RETRY_BACKOFF_FACTOR,
        method_whitelist=['GET', 'POST']
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({
        'Content-Type': 'application/json',
        'User-Agent': 'google-cloud-pipeline-components'
    })
    session.mount('https://', adapter)
    return session

  def _refresh_session_auth(self) -> None:
    """Refreshes credentials in the http session."""
    if not self._creds.valid:
      self._creds.refresh(google.auth.transport.requests.Request())
    self._session.headers.update({
        'Authorization': 'Bearer '+ self._creds.token
    })

  def _write_gcp_resources(self, batch_uri: str, gcp_resources: str) -> None:
    """Write job information to `gcp_resources` output parameter.

    Args:
      batch_uri: The Batch resource uri.
      gcp_resources: File path for storing `gcp_resources` output parameter.
    """
    job_resources = gcp_resources_pb2.GcpResources()
    job_resource = job_resources.resources.add()
    job_resource.resource_type = self._type
    job_resource.resource_uri = batch_uri
    with open(gcp_resources, 'w') as f:
      f.write(json_format.MessageToJson(job_resources))

  def check_if_batch_exists(self, batch_id) -> Optional[str]:
    """Check if the Dataproc batch workload is already created.

    Args:
      batch_id: The Batch resource ID.

    Returns:
      Batch name if it exists.
      None if the Batch resource does not exist.

    Raises:
      RuntimeError: Batch resource uri does not match the job parameters.
      ValueError: Batch resource uri format is invalid.
    """
    if path.exists(self._gcp_resources) and os.stat(self._gcp_resources).st_size != 0:
      with open(self._gcp_resources) as f:
        serialized_gcp_resources = f.read()

      job_resources = json_format.Parse(serialized_gcp_resources,
                                        gcp_resources_pb2.GcpResources())
      # Resources should only contain one item.
      if len(job_resources.resources) != 1:
        raise ValueError(
            f'gcp_resources should contain one resource, found {len(job_resources.resources)}'
        )
      # Validate the format of the resource uri.
      job_name_pattern = re.compile(_DATAPROC_BATCH_NAME_TEMPLATE)
      match = job_name_pattern.match(job_resources.resources[0].resource_uri)
      try:
        matched_project = match.group('project')
        matched_location = match.group('location')
        matched_batch_id = match.group('batch')
      except AttributeError as err:
        raise ValueError('Invalid Dataproc Batch resource uri: {}. Expect: {}.'.format(
            job_resources.resources[0].resource_uri,
            'https://dataproc.googleapis.com/v1/projects/[projectId]/locations/[location]/batches/[batchId]'
        )) from err
      # Validate the job parameters match the existing resource uri.
      if not (self._project == matched_project and
              self._location == matched_location and
              batch_id == matched_batch_id):
        raise RuntimeError('Invalid state: existing resource uri does not match job parameters: {}'
                           .format(job_resources.resources[0].resource_uri))

      job_name = f'projects/{matched_project}/locations/{matched_location}/batches/{matched_batch_id}'
      logging.info('Existing Batch resource found in gcp_resources. Resource name: %s', job_name)
      return job_name
    else:
      return None

  def wait_for_batch(
      self,
      batch_name: str,
      poll_interval_seconds: int
  ) -> Dict[str, Any]:
    """Waits for a Dataproc batch workload to reach a final state.

    Args:
      batch_name: Batch resource name.
      poll_interval_seconds: Seconds to wait between polls.

    Returns:
      Dict of the Batch resource
    """
    batch_uri = f'{_DATAPROC_BATCH_NAME_PREFIX}/{batch_name}'
    while True:
      time.sleep(poll_interval_seconds)
      # Get the Batch resource.
      self._refresh_session_auth()
      batch = self._session.get(batch_uri)
      try:
        batch.raise_for_status()
      except requests.exceptions.HTTPError as err:
        raise RuntimeError('Failed to poll Batch resource: {}'
                           .format(batch_uri)) from err
      # Return the Batch resource when it has reached a 'SUCCEEDED'
      # or 'CANCELLED' state.
      try:
        batch_json = batch.json()
        if batch_json['state'] == 'SUCCEEDED':
          logging.info('Batch completed succesesfully. Resource name: %s.',
                       batch_json['name'])
          return batch_json
        elif batch_json['state'] == 'CANCELLED':
          logging.info('Batch was cancelled. Resource name: %s', batch_json['name'])
          return batch_json
        elif batch_json['state'] == 'FAILED':
          # Fail the pipeline step if the Batch resource has reached a 'FAIL' state.
          raise RuntimeError('Batch failed. Resource name: {}, Error: {}'
                             .format(batch_json['name'], batch_json['state_message']))
      except KeyError as err:
        raise RuntimeError('Failed to get Batch resource state. Resource: {}'
                           .format(batch_json)) from err
      except json.decoder.JSONDecodeError as err:
        raise RuntimeError('Failed to decode JSON from response: {}'
                           .format(batch)) from err

      logging.info('Polled Batch: %s. Batch state: %s.',
                   batch_name, str(batch_json['state']))

  def create_batch(
      self,
      batch_id: str,
      batch_request_json: Dict[str, Any]
  ) -> Dict[str, Any]:
    """Common function for creating a batch workload.

    Args:
      batch_id: Dataproc batch id.
      batch_request_json: A json serialized batch-specific proto. For more details, see:
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch

    Returns:
       Batch resource name.
    """
    try:
      if all(key in batch_request_json['execution_config'] for key in ('network_uri', 'subnetwork_uri')):
        raise AttributeError('Job payload specify either "network_uri" or "subnetwork_uri, not both. Exiting.')
    except KeyError:
      pass

    # Create the Batch resource.
    self._refresh_session_auth()
    create_batch_url = f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={batch_id}'
    operation = self._session.post(
        url=create_batch_url, data=json.dumps(batch_request_json))
    try:
      operation.raise_for_status()
    except requests.exceptions.HTTPError as err:
      raise RuntimeError('Failed to create Batch resource: {}'
                         .format(create_batch_url)) from err
    # Get the Batch resource uri from the Operation resource.
    try:
      operation_json = operation.json()
      batch_name = operation_json['metadata']['batch']
      batch_uri = f'{_DATAPROC_BATCH_NAME_PREFIX}/{batch_name}'
    except json.decoder.JSONDecodeError as err:
      raise RuntimeError('Failed to decode JSON from response: {}'
                         .format(operation)) from err
    except KeyError as err:
      raise RuntimeError('Failed to read Batch resource name from response: {}.'
                         .format(operation)) from err
    # Write the Batch resource uri to the gcp_resources output file.
    self._write_gcp_resources(batch_uri, self._gcp_resources)
    return batch_name


def _create_batch(
    type: str,
    project: str,
    location: str,
    batch_id: str,
    batch_request_json: Dict[str, Any],
    gcp_resources: str,
) -> Dict[str, Any]:
  """Common function for creating Dataproc Batch workloads.

  Args:
    type: Dataproc job type that is written to gcp_resources.
    project: Project to launch the batch workload.
    location: Location to launch the batch workload.
    batch_id: Dataproc batch id.
    batch_request_json: Dict of the Batch resource. For more details, see:
      https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch
    gcp_resources: File path for storing `gcp_resources` output parameter.

  Returns:
    Dict representation of the created Batch resource.
  """
  remote_runner = DataprocBatchRemoteRunner(type, project, location, gcp_resources)
  batch_name = remote_runner.check_if_batch_exists(batch_id)
  if not batch_name:
    batch_name = remote_runner.create_batch(batch_id, batch_request_json)
  # Wait for the Batch workload to finish.
  return remote_runner.wait_for_batch(batch_name, _POLL_INTERVAL_SECONDS)


def create_pyspark_batch(
    type: str,
    project: str,
    location: str,
    batch_id: str,
    payload: Optional[str],
    gcp_resources: str,
) -> None:
  """Create a PySpark batch workload.

  Args:
    type: Dataproc job type that is written to gcp_resources.
    project: Project to launch the batch workload.
    location: Location to launch the batch workload.
    batch_id: Dataproc batch id.
    payload: A json serialized batch-specific proto. For more details, see:
      https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch
    gcp_resources: File path for storing `gcp_resources` output parameter.
  """
  batch_request_json = json_util.recursive_remove_empty(
      json.loads(payload, strict=False))
  _create_batch(type, project, location, batch_id, batch_request_json, gcp_resources)


def create_spark_batch(
    type: str,
    project: str,
    location: str,
    batch_id: str,
    payload: Optional[str],
    gcp_resources: str,
) -> None:
  """Create a Spark batch workload.

  Args:
    type: Dataproc job type that is written to gcp_resources.
    project: Project to launch the batch workload.
    location: Location to launch the batch workload.
    batch_id: Dataproc batch id.
    payload: A json serialized batch-specific proto. For more details, see:
      https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch
    gcp_resources: File path for storing `gcp_resources` output parameter.
  """
  batch_request_json = json_util.recursive_remove_empty(
      json.loads(payload, strict=False))
  try:
    # Check that spark_batch union fields contain only one valid value.
    if all(key in batch_request_json['spark_batch'] for key in ('main_class', 'main_jar_file_uri')):
      raise AttributeError('Job payload specify either "main_class" or "main_jar_file_uri, not both. Exiting.')
  except KeyError as err:
    raise AttributeError('Job payload must include a "spark_batch" configuration. Exiting.') from err
  _create_batch(type, project, location, batch_id, batch_request_json, gcp_resources)


def create_spark_r_batch(
    type: str,
    project: str,
    location: str,
    batch_id: str,
    payload: Optional[str],
    gcp_resources: str,
) -> None:
  """Create a Spark R batch workload.

  Args:
    type: Dataproc job type that is written to gcp_resources.
    project: Project to launch the batch workload.
    location: Location to launch the batch workload.
    batch_id: Dataproc batch id.
    payload: A json serialized batch-specific proto. For more details, see:
      https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch
    gcp_resources: File path for storing `gcp_resources` output parameter.
  """
  batch_request_json = json_util.recursive_remove_empty(
      json.loads(payload, strict=False))
  _create_batch(type, project, location, batch_id, batch_request_json, gcp_resources)


def create_spark_sql_batch(
    type: str,
    project: str,
    location: str,
    batch_id: str,
    payload: Optional[str],
    gcp_resources: str,
) -> None:
  """Create a Spark SQL batch workload.

  Args:
    type: Dataproc job type that is written to gcp_resources.
    project: Project to launch the batch workload.
    location: Location to launch the batch workload.
    batch_id: Dataproc batch id.
    payload: A json serialized batch-specific proto. For more details, see:
      https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.locations.batches#:-batch
    gcp_resources: File path for storing `gcp_resources` output parameter.
  """
  batch_request_json = json_util.recursive_remove_empty(
      json.loads(payload, strict=False))
  _create_batch(type, project, location, batch_id, batch_request_json, gcp_resources)
