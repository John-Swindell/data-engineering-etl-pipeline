"""
Caching Infrastructure Unit Tests

This module validates the two-tier caching architecture (Local Disk + Cloud Storage).
It utilizes `unittest.mock` to simulate Google Cloud Storage interactions, ensuring
that core logic regarding cache hits, misses, and serialization can be verified
in offline or CI/CD environments without requiring live GCP credentials.

Author: John Swindell
"""

import unittest
import os
import sys
import shutil
import pandas as pd
from unittest.mock import MagicMock

# --- Path Configuration ---
# Ensure the shared pipeline modules are importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(PARENT_DIR)

from pipeline_helpers import GCSCachingManager

class TestGCSCachingManager(unittest.TestCase):

    def setUp(self):
        """Initialize a clean environment with mocked GCS clients before each test."""
        self.project_id = 'test-project'
        self.bucket_name = 'test-bucket'
        self.local_cache_dir = os.path.join(SCRIPT_DIR, 'temp_test_cache')

        # 1. Mock the GCS Client Hierarchy
        self.mock_storage_client = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_blob = MagicMock()

        self.mock_storage_client.bucket.return_value = self.mock_bucket
        self.mock_bucket.blob.return_value = self.mock_blob

        # 2. Initialize the Manager with the Mock
        self.cacher = GCSCachingManager(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            local_cache_dir=self.local_cache_dir,
            gcs_client=self.mock_storage_client
        )

        # 3. Ensure clean local state
        self._clean_local_dir()
        os.makedirs(self.local_cache_dir, exist_ok=True)

    def tearDown(self):
        """Cleanup local artifacts after each test."""
        self._clean_local_dir()

    def _clean_local_dir(self):
        """Helper to remove the temporary test directory."""
        if os.path.exists(self.local_cache_dir):
            shutil.rmtree(self.local_cache_dir)

    def test_cache_miss(self):
        """
        Scenario: File does not exist locally or in the cloud.
        Expected: Returns None; Does not attempt download.
        """
        # Setup Mock
        self.mock_blob.exists.return_value = False

        # Execute
        result = self.cacher.get('non_existent_file.parquet')

        # Verify
        self.assertIsNone(result)
        self.mock_blob.download_to_filename.assert_not_called()

    def test_cache_hit_from_cloud(self):
        """
        Scenario: File is missing locally but exists in the cloud.
        Expected: Downloads file from Cloud, saves locally, returns DataFrame.
        """
        # Setup Mock
        self.mock_blob.exists.return_value = True
        local_path = os.path.join(self.local_cache_dir, 'test_data.parquet')

        # Define side_effect to simulate the download creating a file
        def simulate_gcs_download(destination_path):
            df = pd.DataFrame({'col1': [10, 20], 'col2': ['a', 'b']})
            df.to_parquet(destination_path)

        self.mock_blob.download_to_filename.side_effect = simulate_gcs_download

        # Execute
        result = self.cacher.get('test_data.parquet')

        # Verify
        self.mock_blob.download_to_filename.assert_called_once_with(local_path)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (2, 2))

    def test_cache_write_strategy(self):
        """
        Scenario: Saving data to the cache.
        Expected: Serializes to local disk AND uploads to Cloud Storage.
        """
        # Setup Data
        data_to_cache = pd.DataFrame({'value': [100, 200, 300]})
        filename = 'new_dataset.parquet'
        expected_local_path = os.path.join(self.local_cache_dir, filename)

        # Execute
        self.cacher.set(filename, data_to_cache)

        # Verify Local Persistence
        self.assertTrue(os.path.exists(expected_local_path))

        # Verify Cloud Upload
        self.mock_bucket.blob.assert_called_once_with(filename)
        self.mock_blob.upload_from_filename.assert_called_once_with(expected_local_path)

if __name__ == '__main__':
    unittest.main()