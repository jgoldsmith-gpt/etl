from datetime import datetime
import luigi
import os

import numpy as np
import pandas as pd
import pytz

from oeem_etl import config
from oeem_etl.paths import mirror_path
from oeem_etl import constants
from oeem_etl.requester import Requester
from oeem_etl.datastore_utils import loaded_project_ids, loaded_trace_ids


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


def bulk_load_trace_csv(f, method="upsert"):
    requester = Requester(config.oeem.url, config.oeem.access_token)
    data = pd.read_csv(f, dtype=str).to_dict('records')

    unique_traces = list(set([
        (d["trace_id"], d["interpretation"], d["unit"]) for d in data
    ]))

    trace_data = [
        {
            "trace_id": trace[0],
            "interpretation": trace[1],
            "unit": trace[2],
            "added": datetime.utcnow().isoformat(),
            "updated": datetime.utcnow().isoformat(),
        } for trace in unique_traces
    ]

    trace_response = requester.post(
        constants.TRACE_BULK_UPSERT_VERBOSE_URL, trace_data)

    trace_pks_by_id = {
        record["trace_id"]: record["id"]
        for record in trace_response.json()
    }

    def maybe_float(value):
        try: return float(value)
        except: return np.nan

    trace_record_data = [
        {
            "trace_id": trace_pks_by_id[record["trace_id"]],
            "value": maybe_float(record["value"]),
            "start": record["start"],
            "estimated": record["estimated"],
        }
        for record in data
    ]

    if method == "upsert":
        trace_record_response = requester.post(
            constants.TRACE_RECORD_BULK_UPSERT_URL, trace_record_data)
    elif method == "insert":
        trace_record_response = requester.post(
            constants.TRACE_RECORD_BULK_INSERT_URL, trace_record_data)
    return trace_record_response.status_code == 200


def bulk_load_project_trace_mapping_csv(f):
    requester = Requester(config.oeem.url, config.oeem.access_token)

    trace_ids = {d["trace_id"]: d["id"] for d in loaded_trace_ids()}
    project_ids = {d["project_id"]: d["id"] for d in loaded_project_ids()}

    raw_matches = pd.read_csv(f, dtype=str).to_dict('records')

    data = []
    for match in raw_matches:
        trace_id = trace_ids.get(match["trace_id"], None)
        project_id = project_ids.get(match["project_id"], None)
        if trace_id is not None and project_id is not None:
            data.append({
                "trace_id": trace_id,
                "project_id": project_id
            })

    response = requester.post(
        constants.PROJECT_TRACE_MAPPING_BULK_UPSERT_VERBOSE_URL, data)

    return response.status_code == 201

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


class LoadProjects(luigi.WrapperTask):
    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        return [LoadProjectCSV(path) for path in paths]


class LoadTraceCSV(luigi.Task):
    path = luigi.Parameter()
    method = luigi.Parameter(default="upsert")

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_trace_csv(self.input().open('r'), self.method)
        if success:
            self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadTraces(luigi.WrapperTask):
    method = luigi.Parameter(default="upsert")

    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_TRACE_OUTPUT_DIR)
        return [LoadTraceCSV(path, self.method) for path in paths]


class LoadProjectTraceMappingCSV(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_project_trace_mapping_csv(self.input().open('r'))

        if success:
            self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadProjectTraceMappings(luigi.WrapperTask):

    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_PROJECT_TRACE_MAPPING_OUTPUT_DIR)
        return [LoadProjectTraceMappingCSV(path) for path in paths]
