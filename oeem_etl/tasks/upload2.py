"""
Upload tasks to be imported in client-specific parsers.

Designed to replace the tasks in `upload.py`.

Expects files to be laid out in the following directory structure:

    /oeem-format
        /projects
        /consumptions

There can nested subdirectories in `projects` and `consumptions`.

Top-level tasks:

    LoadProjects
    UploadConsumptions
    LoadAll

"""

import os
import pandas as pd
import numpy as np
import luigi
from oeem_etl.uploader import upload_project_dataframe, upload_consumption_dataframe_faster
import oeem_etl.config as config
from oeem_etl.paths import mirror_path


class FetchFile(luigi.ExternalTask):
    """Fetchs file from either local disk or cloud storage"""
    raw_file_path = luigi.Parameter()

    def output(self):
        return config.oeem.target_class(self.raw_file_path)


class UploadProject(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return FetchFile(self.path)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_project, config.oeem.datastore)
        self.write_flag()

    def output(self):
        uploaded_path = formatted2uploaded_path(self.path)
        return config.oeem.flag_target_class(uploaded_path)


class LoadProjects(luigi.WrapperTask):
    def requires(self):
        project_paths = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        return [UploadProject(path) for path in project_paths]


def formatted2uploaded_path(path):
    return mirror_path(path,
                       config.oeem.full_path(config.oeem.formatted_base_path),
                       config.oeem.full_path(config.oeem.uploaded_base_path))


class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    project_paths = luigi.ListParameter()

    def requires(self):
        return {
            'file': FetchFile(self.path),
            'projects': LoadProjects()
        }

    def load_dataset(self):

        def maybe_float(value):
            try:
                return float(value)
            except:
                return np.nan

        df = pd.read_csv(self.input()['file'].open('r'), dtype={
            'project_id': str,
        }, converters={
            'value': maybe_float
        })
        df = df.dropna()
        return df

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe_faster(parsed_consumption, config.oeem.datastore)
        self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadAll(luigi.WrapperTask):

    pattern = luigi.Parameter(default='', description="Only upload consumption files that contain the string `pattern`")

    def requires(self):

        project_paths     = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        consumption_paths = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_CONSUMPTIONS_OUTPUT_DIR)

        def filter_paths(paths, apply_pattern=False):
            paths = [path for path in paths if not path.endswith('.DS_Store')]
            if self.pattern and apply_pattern:
                paths = [path for path in paths if self.pattern in path]
            return paths

        project_paths = filter_paths(project_paths)
        consumption_paths = filter_paths(consumption_paths, apply_pattern=True)

        return [
            UploadConsumption(path, project_paths)
            for path in consumption_paths
        ]
