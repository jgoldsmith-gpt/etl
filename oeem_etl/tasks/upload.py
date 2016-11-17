from datetime import datetime
import luigi
import os
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import numpy as np
import pandas as pd
import pytz
import csv
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


def direct_load_records(records, db_table):
    """Using an existing database connection, directly load list of data into table"""

    conn = config.oeem.database_conn
    cursor = conn.cursor()

    # Write the request data to an in-memory CSV file for a subsequent COPY
    # TODO: support loading directly from a file (for now, we munge file contents
    # a bit in core ETL, which makes it more convenient to load from list of
    # records.)
    infile = StringIO()
    fieldnames = records[0].keys()
    writer = csv.DictWriter(
        infile, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
    def encode(value):
        # tests for Python2
        if 'unicode' in __builtins__:
            try:
                # if it's a string, explicity encode
                return value.encode('utf-8')
            except:
                # not a string
                return value
        else:
            # no need to encode in Python3
            return value
    for record in records:
        writer.writerow({k: encode(v) for k, v in record.items()})
    infile.seek(0)

    cursor.copy_from(file=infile, table=db_table, sep='\t', columns=fieldnames, null="")
    conn.commit()

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

    # Reshape
    data = []
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

    response = requester.post(constants.PROJECT_METADATA_BULK_UPSERT_URL, data)
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
        response = trace_record_response.status_code == 200
    elif method == "insert":
        trace_record_response = requester.post(
            constants.TRACE_RECORD_BULK_INSERT_URL, trace_record_data)
        response = trace_record_response.status_code == 200
    elif method == "direct":
        direct_load_records(trace_record_data, 'datastore_tracerecord')
        response = True

    return response


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
        return [LoadProjectCSV(path) for path in paths if not path.endswith(".DS_Store")]


conn = None

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




