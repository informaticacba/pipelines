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
"""Test Dataproc Batch Remote Runner module."""

import json
import os
import time
from unittest import mock

import google.auth
from google_cloud_pipeline_components.container.experimental.gcp_launcher import dataproc_batch_remote_runner
from google_cloud_pipeline_components.proto import gcp_resources_pb2
import requests

from google.protobuf import json_format
import unittest


class DataprocBatchRemoteRunnerUtilsTests(unittest.TestCase):
  """Test cases for Dataproc Batch remote runner."""

  def setUp(self):
    super(DataprocBatchRemoteRunnerUtilsTests, self).setUp()
    self._project = 'test-project'
    self._location = 'test-location'
    self._container_image = 'test-container-image'
    self._batch_id = 'test-batch-id'
    self._labels = {'foo': 'bar', 'fizz': 'buzz'}
    self._creds_token = 'fake_token'
    self._operation_id = 'fake-operation-id'
    self._headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + self._creds_token,
        'User-Agent': 'google-cloud-pipeline-components'
    }
    self._batch_name = f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    self._batch_uri = f'https://dataproc.googleapis.com/v1/projects/{self._batch_name}'
    self._gcp_resources = os.path.join(
        os.getenv('TEST_UNDECLARED_OUTPUTS_DIR'), 'gcp_resources')

    self._test_spark_batch = {
        'spark_batch': {
            'main_class': 'com.example.testMainClass',
            'jar_file_uris': ['testJarFileUri1', 'testJarFileUri2'],
            'file_uris': ['testFileUri1', 'testFileUri2'],
            'archive_uris': ['testArchiveUri1', 'testArchiveUri2'],
            'args': ['arg1', 'arg2']
        },
        'labels': self._labels
    }
    self._test_pyspark_batch = {
        'pyspark_batch': {
            'main_python_file_uri': 'test-python-file',
            'python_file_uris': ['testPythonFileUri1', 'testPythonFileUri2'],
            'jar_file_uris': ['test-jar-uri-1', 'testJarFileUri2'],
            'file_uris': ['testFileUri1', 'testFileUri2'],
            'archive_uris': ['testArchiveUri1', 'testArchiveUri2'],
            'args': ['arg1', 'arg2']
        },
        'labels': self._labels
    }
    self._test_spark_r_batch = {
        'spark_r_batch': {
            'main_r_file_uri': 'test-r-file',
            'file_uris': ['testFileUri1', 'testFileUri2'],
            'archive_uris': ['testArchiveUri1', 'testArchiveUri2'],
            'args': ['arg1', 'arg2']
        },
        'labels': self._labels
    }
    self._test_spark_sql_batch = {
        'spark_r_batch': {
            'main_r_file_uri': 'test-r-file',
            'file_uris': ['testFileUri1', 'testFileUri2'],
            'archive_uris': ['testArchiveUri1', 'testArchiveUri2'],
            'args': ['arg1', 'arg2']
        },
        'labels': self._labels
    }

  def tearDown(self):
    super(DataprocBatchRemoteRunnerUtilsTests, self).tearDown()
    if os.path.exists(self._gcp_resources):
      os.remove(self._gcp_resources)

  def _validate_gcp_resources_succeeded(self):
    """Test the gcp_resources output parameter."""
    with open(self._gcp_resources) as f:
      serialized_gcp_resources = f.read()
      # Instantiate GCPResources Proto
      batch_resources = json_format.Parse(serialized_gcp_resources,
                                          gcp_resources_pb2.GcpResources())
      self.assertLen(batch_resources.resources, 1)
      self.assertEqual(
          batch_resources.resources[0].resource_uri,
          f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
      )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_poll_existing_batch_succeeded(self, mock_time_sleep,
                                         mock_get_requests, _,
                                         mock_auth_default):
    """Test polling of a batch workload that has reached a succeeded state."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          f'{{"resources": [{{"resourceType": "PySparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}"}}]}}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_batch(
        type='SparkBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_batch),
        gcp_resources=self._gcp_resources
    )

    mock_time_sleep.assert_called_once()
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_poll_existing_batch_failed(self, mock_time_sleep,
                                      mock_get_requests, _,
                                      mock_auth_default):
    """Test polling of a batch workload that has reached a failed state."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          f'{{"resources": [{{"resourceType": "PySparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}"}}]}}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'FAILED',
        'state_message': 'Google Cloud Dataproc Agent reports job failure.'
    }
    mock_get_requests.return_value = mock_polled_batch

    with self.assertRaises(RuntimeError):
      dataproc_batch_remote_runner.create_spark_batch(
          type='SparkBatch',
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          payload=json.dumps(self._test_spark_batch),
          gcp_resources=self._gcp_resources
      )

    mock_time_sleep.assert_called_once()
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_poll_existing_batch_not_found(self, mock_time_sleep,
                                         mock_get_requests, _,
                                         mock_auth_default):
    """Test polling of a batch workload that is not found."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          f'{{"resources": [{{"resourceType": "PySparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}"}}]}}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_polled_batch = mock.Mock(spec=requests.models.Response)
    mock_polled_batch.raise_for_status.side_effect = requests.exceptions.HTTPError(mock.Mock(status=404), 'Not found')
    mock_get_requests.return_value = mock_polled_batch

    with self.assertRaises(RuntimeError):
      dataproc_batch_remote_runner.create_spark_batch(
          type='SparkBatch',
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          payload=json.dumps(self._test_spark_batch),
          gcp_resources=self._gcp_resources
      )

    mock_time_sleep.assert_called_once()
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_batch_poll_existing_batch_cancelled(self, mock_time_sleep,
                                               mock_get_requests, _,
                                               mock_auth_default):
    """Test polling of a batch workload that has reached a cancelled state."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          f'{{"resources": [{{"resourceType": "PySparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}"}}]}}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'CANCELLED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_batch(
        type='SparkBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_batch),
        gcp_resources=self._gcp_resources
    )

    mock_time_sleep.assert_called_once()
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )

    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  def test_batch_exists_wrong_format(self, mock_auth_default):
    """Test when the batch workload exists but the gcp_resources file contains an invalid resource uri."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          '{"resources": [{"resourceType": "PySparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/test-project/locations/test-location/batches"}]}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    with self.assertRaises(ValueError):
      dataproc_batch_remote_runner.create_spark_batch(
          type='SparkBatch',
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          payload=json.dumps(self._test_spark_batch),
          gcp_resources=self._gcp_resources
      )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_spark_batch_remote_runner_succeeded(self, mock_time_sleep,
                                               mock_post_requests, mock_get_requests,
                                               _, mock_auth_default):
    """Test SparkBatch creation where the workload reaches a succeeded state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_batch(
        type='SparkBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_batch),
        gcp_resources=self._gcp_resources
    )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_pyspark_batch_remote_runner_succeeded(self, mock_time_sleep,
                                                 mock_post_requests, mock_get_requests,
                                                 _, mock_auth_default):
    """Test PySparkBatch creation where the workload reaches a succeeded state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_pyspark_batch(
        type='PySparkBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_pyspark_batch),
        gcp_resources=self._gcp_resources
    )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_pyspark_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_spark_r_batch_remote_runner_succeeded(self, mock_time_sleep,
                                                 mock_post_requests, mock_get_requests,
                                                 _, mock_auth_default):
    """Test SparkRBatch creation where the workload reaches a succeeded state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_r_batch(
        type='SparkRBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_r_batch),
        gcp_resources=self._gcp_resources
    )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_r_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_spark_sql_batch_remote_runner_succeeded(self, mock_time_sleep,
                                                   mock_post_requests, mock_get_requests,
                                                   _, mock_auth_default):
    """Test SparkSqlBatch creation where the workload reaches a succeeded state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_sql_batch(
        type='SparkSqlBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_sql_batch),
        gcp_resources=self._gcp_resources
    )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_sql_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_spark_batch_remote_runner_failed(self, mock_time_sleep,
                                            mock_post_requests, mock_get_requests,
                                            _, mock_auth_default):
    """Test SparkRatch creation where the workload reaches a failed state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'FAILED'
    }
    mock_get_requests.return_value = mock_polled_batch

    with self.assertRaises(RuntimeError):
      dataproc_batch_remote_runner.create_spark_batch(
          type='SparkBatch',
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          payload=json.dumps(self._test_spark_batch),
          gcp_resources=self._gcp_resources
      )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(time, 'sleep', autospec=True)
  def test_spark_batch_remote_runner_cancelled(self, mock_time_sleep,
                                               mock_post_requests, mock_get_requests,
                                               _, mock_auth_default):
    """Test SparkRatch creation where the workload reaches a failed state."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'CANCELLED'
    }
    mock_get_requests.return_value = mock_polled_batch

    dataproc_batch_remote_runner.create_spark_batch(
        type='SparkBatch',
        project=self._project,
        location=self._location,
        batch_id=self._batch_id,
        payload=json.dumps(self._test_spark_batch),
        gcp_resources=self._gcp_resources
    )

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_batch)
    )
    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_time_sleep.assert_called_once()
    self._validate_gcp_resources_succeeded()

  def test_spark_batch_remote_runner_oneof_field_violation(self):
    """Test SparkBatch workload creation with a 'oneof' field violation."""
    batch_request_json = {
        'spark_batch': {
            'main_class': 'com.example.testMainClass',
            'main_jar_file_uri': 'testMainJarFileUri',
            'jar_file_uris': ['testJarFileUri1', 'testJarFileUri2'],
            'file_uris': ['testFileUri1', 'testFileUri2'],
            'archive_uris': ['testArchiveUri1', 'testArchiveUri2'],
            'args': ['arg1', 'arg2']
        },
        'labels': self._labels
    }

    with self.assertRaises(AttributeError):
      dataproc_batch_remote_runner.create_spark_batch(
          type='SparkBatch',
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          payload=json.dumps(batch_request_json),
          gcp_resources=self._gcp_resources
      )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'post', autospec=True)
  @mock.patch.object(dataproc_batch_remote_runner.DataprocBatchRemoteRunner, '_refresh_session_auth', autospec=True)
  def test_http_post_request_called_with_headers(self, mock_refresh_session_auth,
                                                 mock_post_requests, _,
                                                 mock_auth_default):
    """Test that http authorization headers are refreshed for POST requests."""
    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_operation = mock.Mock(spec=requests.models.Response)
    mock_operation.json.return_value = {
        'name': f'projects/{self._project}/regions/{self._location}/operations/{self._operation_id}',
        'metadata': {
            'batch': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
        }
    }
    mock_post_requests.return_value = mock_operation

    remote_runner = dataproc_batch_remote_runner.DataprocBatchRemoteRunner(
        'SparkBatch', self._project, self._location, self._gcp_resources
    )
    remote_runner.create_batch(self._batch_id, self._test_spark_batch)

    mock_post_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/?batchId={self._batch_id}',
        data=json.dumps(self._test_spark_batch)
    )
    mock_refresh_session_auth.assert_called_once()
    self._validate_gcp_resources_succeeded()

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(google.auth.transport.requests, 'Request', autospec=True)
  @mock.patch.object(requests.sessions.Session, 'get', autospec=True)
  @mock.patch.object(dataproc_batch_remote_runner.DataprocBatchRemoteRunner, '_refresh_session_auth', autospec=True)
  def test_http_get_request_called_with_headers(self, mock_refresh_session_auth,
                                                mock_get_requests, _,
                                                mock_auth_default):
    """Test that http authorization headers are refreshed for GET requests."""
    with open(self._gcp_resources, 'w') as f:
      f.write(
          f'{{"resources": [{{"resourceType": "SparkJob", "resourceUri": "https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}"}}]}}'
      )

    mock_creds = mock.Mock()
    mock_creds.token = self._creds_token
    mock_auth_default.return_value = [mock_creds, 'project']

    mock_polled_batch = mock.Mock()
    mock_polled_batch.json.return_value = {
        'name': f'projects/{self._project}/locations/{self._location}/batches/{self._batch_id}',
        'state': 'SUCCEEDED'
    }
    mock_get_requests.return_value = mock_polled_batch

    remote_runner = dataproc_batch_remote_runner.DataprocBatchRemoteRunner(
        'SparkBatch', self._project, self._location, self._gcp_resources
    )
    remote_runner.wait_for_batch(self._batch_name, 0)

    mock_get_requests.assert_called_once_with(
        self=mock.ANY,
        url=f'https://dataproc.googleapis.com/v1/projects/{self._project}/locations/{self._location}/batches/{self._batch_id}'
    )
    mock_refresh_session_auth.assert_called_once()
    self._validate_gcp_resources_succeeded()
