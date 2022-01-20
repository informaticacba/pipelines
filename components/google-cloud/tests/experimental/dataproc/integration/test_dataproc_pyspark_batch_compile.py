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
"""Test google-cloud-pipeline-components to ensure they compile correctly."""

import json
import os
from google_cloud_pipeline_components.experimental.dataproc import DataprocPySparkBatchOp
import kfp
from kfp.v2 import compiler
import unittest


class ComponentsCompileTest(unittest.TestCase):
  """Pipeline compilation tests cases for Dataproc Batch components."""

  def setUp(self):
    super(ComponentsCompileTest, self).setUp()
    self._project = 'test-project'
    self._location = 'test-location'
    self._batch_id = 'test-batch-id'
    self._labels = {'foo': 'bar', 'fizz': 'buzz'}
    self._main_python_file_uri = 'main-python-file-uri'
    self._package_path = os.path.join(
        os.getenv('TEST_UNDECLARED_OUTPUTS_DIR'), 'pipeline.json')

  def tearDown(self):
    super(ComponentsCompileTest, self).tearDown()
    if os.path.exists(self._package_path):
      os.remove(self._package_path)

  def test_dataproc_create_pyspark_batch_op_compile(self):
    """Compile a test pipeline using the Dataproc PySparkBatch component."""
    @kfp.dsl.pipeline(name='dataproc-batch-test')
    def pipeline():
      DataprocPySparkBatchOp(
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          main_python_file_uri=self._main_python_file_uri,
          labels=self._labels)

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=self._package_path)
    with open(self._package_path) as f:
      executor_output_json = json.load(f, strict=False)

    with open('testdata/dataproc_create_pyspark_batch_component_pipeline.json') as ef:
      expected_executor_output_json = json.load(ef, strict=False)

    # Ignore the kfp SDK & schema version during comparison
    del executor_output_json['sdkVersion']
    del executor_output_json['schemaVersion']
    self.assertDictEqual(executor_output_json, expected_executor_output_json)
