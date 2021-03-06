"""
Load configuration using the "Parameters from config Ingestion" pattern.

http://luigi.readthedocs.io/en/stable/configuration.html#parameters-from-config-ingestion
"""

import luigi
import os
from oeem_etl.storage import StorageClient


class oeem(luigi.Config):
    url                  = luigi.Parameter()
    access_token         = luigi.Parameter()
    project_owner        = luigi.IntParameter()
    file_storage         = luigi.Parameter()
    local_data_directory = luigi.Parameter()
    uploaded_base_path   = luigi.Parameter()
    formatted_base_path  = luigi.Parameter()

    OEEM_FORMAT_OUTPUT_BASE_PATH = luigi.Parameter(default='oeem-format')

    _target_class = None
    _flag_target_class = None
    _storage = None
    _datastore = None

    def __init__(self):
        super(oeem, self).__init__()

        self.OEEM_FORMAT_PROJECT_OUTPUT_DIR = \
            os.path.join(str(self.OEEM_FORMAT_OUTPUT_BASE_PATH), 'projects')

        self.OEEM_FORMAT_TRACE_OUTPUT_DIR = \
            os.path.join(str(self.OEEM_FORMAT_OUTPUT_BASE_PATH), 'traces')

        self.OEEM_FORMAT_PROJECT_TRACE_MAPPING_OUTPUT_DIR = \
            os.path.join(str(self.OEEM_FORMAT_OUTPUT_BASE_PATH), 'project-trace-mappings')

        self.OEEM_FORMAT_PROJECT_TRACE_MAPPING_PATH = \
            os.path.join(self.OEEM_FORMAT_PROJECT_TRACE_MAPPING_OUTPUT_DIR, 'mapping.csv')

        self.OEEM_FORMAT_PROJECTS_PATH = \
            os.path.join(self.OEEM_FORMAT_PROJECT_OUTPUT_DIR, 'projects.csv')

        self.OEEM_FORMAT_PROJECT_METADATA_PATH = \
            os.path.join(self.OEEM_FORMAT_OUTPUT_BASE_PATH, 'project-metadata.csv')

        self.OEEM_FORMAT_TRACE_BLACKLIST_PATH = \
            os.path.join(self.OEEM_FORMAT_OUTPUT_BASE_PATH, 'trace-blacklist.csv')

    @property
    def storage(self):
        if self._storage is None:
            self._storage = StorageClient(self.__dict__)
        return self._storage

    @property
    def target_class(self):
        if self._target_class is None:
            self._target_class = self.storage.get_target_class()
        return self._target_class

    @property
    def flag_target_class(self):
        if self._flag_target_class is None:
            self._flag_target_class = self.storage.get_flag_target_class()
        return self._flag_target_class

    @property
    def datastore(self):
        # Legacy naming: `datastore` refers to the dict of datastore configuration
        # properties. These now live in the `config.oeem` section.
        if self._datastore is None:
            self._datastore = self.__dict__
        return self._datastore

    def full_path(self, filename):
        return os.path.join(self.local_data_directory, filename)


# If config files are missing, don't die right away
# Useful for CI
try:
    oeem = oeem()
except:
    import traceback
    print("Skipped loading config with following error:")
    traceback.print_exc()
