from datetime import datetime
import luigi
import os

import numpy as np
import pandas as pd
import pytz

from oeem_etl import config
from oeem_etl.paths import mirror_path
from oeem_etl.uploader import constants
from oeem_etl.uploader.requester import Requester


def bulk_load_project_csv(f):
    requester = Requester(config.oeem.url, config.oeem.access_token)
    data = pd.read_csv(f, dtype=str).to_dict('records')

    for record in data:
        # have to patch in project owner field from config
        record['project_owner_id'] = config.oeem.project_owner

        # have to patch in fields that are normally autopopulated
        record['added'] = datetime.utcnow().isoformat()
        record['updated'] = datetime.utcnow().isoformat()

    # only support upsert for now
    response = requester.post(constants.PROJECT_BULK_UPSERT_URL, data)
    return response.status_code == 200


def bulk_load_trace_csv(f):

    requester = Requester(config.oeem.url, config.oeem.access_token)
    data = pd.read_csv(f, dtype=str).to_dict('records')
    import pdb; pdb.set_trace()

    # def load_dataset(self):
    #
    #     def maybe_float(value):
    #         try: return float(value)
    #         except: return np.nan
    #
    #     df = pd.read_csv(self.input()['file'].open('r'),
    #                      dtype={'project_id': str},
    #                      converters={'value': maybe_float})
    #     df = df.dropna()
    #     return df


def formatted2uploaded_path(path):
    return mirror_path(path,
                       config.oeem.full_path(config.oeem.formatted_base_path),
                       config.oeem.full_path(config.oeem.uploaded_base_path))


class FetchFile(luigi.ExternalTask):
    """Fetchs file from either local disk or cloud storage"""
    raw_file_path = luigi.Parameter()

    def output(self):
        return config.oeem.target_class(self.raw_file_path)


class LoadProjectCSV(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(
            os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_project_csv(self.input().open('r'))
        if success:
            self.write_flag()

    def output(self):
        uploaded_path = formatted2uploaded_path(self.path)
        return config.oeem.flag_target_class(uploaded_path)


class LoadProjects3(luigi.WrapperTask):
    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        return [LoadProjectCSV(path) for path in paths]


class LoadTraceCSV(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_trace_csv(self.input().open('r'))
        if success:
            self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadTraces3(luigi.WrapperTask):
    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_CONSUMPTIONS_OUTPUT_DIR)
        return [LoadTraceCSV(path) for path in paths]
