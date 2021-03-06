from datetime import datetime
import luigi
import os

import numpy as np
import pandas as pd
import pytz
from tqdm import tqdm

from oeem_etl import config
from oeem_etl.paths import mirror_path
from oeem_etl import constants
from oeem_etl.requester import Requester
from oeem_etl.datastore_utils import loaded_project_ids, loaded_trace_ids
from oeem_etl.csvs import read_csv_file

def batches(items, batch_size):
    """Helper for splitting a list of items into smaller batches"""
    for i in xrange(0, len(items), batch_size):
        yield items[i:i + batch_size]

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

def bulk_load_project_metadata_csv(f):
    requester = Requester(config.oeem.url, config.oeem.access_token)

    input_data = read_csv_file(f)

    if len(input_data) == 0:
        print("No data to upload.")
        return True

    # auto-detect wide/tall (pivoted, unpivoted) format
    columns = input_data[0].keys()
    data = []
    if set(columns) == set(['project_id', 'key', 'value']):
        # tall format
        for row in input_data:
            key = row['key']
            value = row['value']
            if value is None or value.strip() == '':
                continue
            data.append({
                'project_id': row['project_id'],
                'key': key.decode('utf-8').encode('utf-8'),
                'value': value.decode('utf-8').encode('utf-8')
            })
    else:
        # wide format
        for row in input_data:
            for key, value in row.items():
                if value is None:
                    continue
                if value.strip() == '':
                    continue
                if key == 'project_id':
                    continue
                data.append({
                    'project_id': row['project_id'],
                    'key': key.decode('utf-8').encode('utf-8'),
                    'value': value.decode('utf-8').encode('utf-8')
                })


    n = len(data)
    batch_size = 500
    print(
        "Uploading {} rows of metadata in {} batches of {}"
        .format(n, n/batch_size, batch_size)
    )

    success = []
    for batch in tqdm(batches(data, batch_size)):
        response = requester.post(constants.PROJECT_METADATA_BULK_UPSERT_URL, batch)
        success.append(response.status_code == 200)
    return all(success)

def bulk_load_trace_csv(f, method="upsert", skip_trace_records=False):
    requester = Requester(config.oeem.url, config.oeem.access_token)

    try:
        data = pd.read_csv(f, dtype=str).to_dict('records')
    except ValueError:
        # Assume this is an empty file error, which is ok
        return True

    unique_traces = list(set([
        (d["trace_id"], d["interpretation"], d["unit"], d.get("interval", None)) for d in data
    ]))

    trace_data = [
        {
            "trace_id": trace[0],
            "interpretation": trace[1],
            "unit": trace[2],
            "interval": trace[3],
            "added": datetime.utcnow().isoformat(),
            "updated": datetime.utcnow().isoformat(),
        } for trace in unique_traces
    ]

    trace_response = requester.post(
        constants.TRACE_BULK_UPSERT_VERBOSE_URL, trace_data)

    if skip_trace_records:
        return trace_response.status_code < 300

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

    for batch in tqdm(batches(data, 800)):
        response = requester.post(
            constants.PROJECT_TRACE_MAPPING_BULK_UPSERT_VERBOSE_URL, batch)

    return response.status_code == 201

def bulk_load_trace_blacklist(f):
    requester = Requester(config.oeem.url, config.oeem.access_token)

    input_data = read_csv_file(f)

    successes = []
    for batch in tqdm(batches(input_data, 500)):
        response = requester.post(
            constants.TRACE_BLACKLIST_UPSERT_VERBOSE_URL, batch)

        successes.append(response.status_code == 201)

    return all(successes)

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
    skip_trace_records = luigi.BoolParameter(
        default=False, 
        description="Only upsert Trace metadata if True")

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_trace_csv(self.input().open('r'), self.method, self.skip_trace_records)
        if success:
            self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadTraces(luigi.WrapperTask):
    method = luigi.Parameter(default="upsert")
    skip_trace_records = luigi.BoolParameter(
        default=False, 
        description="Only upsert Trace metadata if True")

    def requires(self):
        paths = config.oeem.storage.get_existing_paths(
            config.oeem.OEEM_FORMAT_TRACE_OUTPUT_DIR)
        return [
            LoadTraceCSV(path, self.method, skip_trace_records=self.skip_trace_records) 
            for path in paths
        ]


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


class LoadProjectMetadata(luigi.Task):

    @property
    def path(self):
        return config.oeem.full_path(config.oeem.OEEM_FORMAT_PROJECT_METADATA_PATH)

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        with self.output().open('w') as f: pass

    def run(self):
        success = bulk_load_project_metadata_csv(self.input().open('r'))
        if success:
            self.write_flag()

    def output(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        return target

class LoadTraceBlacklist(luigi.Task):

    @property
    def path(self):
        return config.oeem.full_path(config.oeem.OEEM_FORMAT_TRACE_BLACKLIST_PATH)

    def requires(self):
        return FetchFile(self.path)

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f: pass

    def run(self):
        success = bulk_load_trace_blacklist(self.input().open('r'))

        if success:
            self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))

