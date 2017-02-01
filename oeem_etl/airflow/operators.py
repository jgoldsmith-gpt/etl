from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from oeem_etl.requester import Requester
from oeem_etl import constants
import os
import json
import csv
import logging
import dateutil
from datetime import datetime
import glob
import pandas as pd


class LoadProjectCSVOperator(BaseOperator):
    ui_color = '#d1f7bb'

    @apply_defaults
    def __init__(self,
                 filename,
                 datastore_url,
                 access_token,
                 project_owner=1,
                 bulk_size=1000,
                 *args, **kwargs):
        super(LoadProjectCSVOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.datastore_url = datastore_url
        self.access_token = access_token
        self.project_owner = project_owner
        self.bulk_size = bulk_size

    def execute(self, context):
        requester = Requester(self.datastore_url, self.access_token)
        data = pd.read_csv(self.filename, dtype=str).to_dict('records')

        upload_data = []
        row_count = 0
        for record in data:
            record['project_owner_id'] = self.project_owner
            record['added'] = datetime.utcnow().isoformat()
            record['updated'] = datetime.utcnow().isoformat()
            upload_data.append(record)

            if len(upload_data) >= self.bulk_size:
                row_count += len(upload_data)
                logging.info("Loading {} rows, {} so far".format(len(upload_data), row_count))
                response = requester.upload_chunk(constants.PROJECT_BULK_UPSERT_URL, upload_data)
                upload_data = []
                if response.status_code != 200:
                    raise RuntimeError('Bad response attempting to upsert')

        # leftovers
        if len(upload_data) > 0:
            row_count += len(upload_data)
            logging.info("Loading {} rows, {} so far".format(len(upload_data), row_count))
            response = requester.upload_chunk(constants.PROJECT_BULK_UPSERT_URL, upload_data)
            if response.status_code != 200:
                    raise RuntimeError('Bad response attempting to upsert')

        logging.info("Loaded {} project rows".format(len(data)))


class LoadProjectMetadataCSVOperator(BaseOperator):
    ui_color = '#d1f7bb'

    @apply_defaults
    def __init__(self,
                 filename,
                 datastore_url,
                 access_token,
                 bulk_size=1000,
                 *args, **kwargs):
        super(LoadProjectMetadataCSVOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.datastore_url = datastore_url
        self.access_token = access_token
        self.bulk_size = bulk_size

    def execute(self, context):
        upload_data = []
        rows_loaded = 0
        requester = Requester(self.datastore_url, self.access_token)
        with open(self.filename, 'r') as f_in:
            reader = csv.DictReader(f_in, skipinitialspace=True)
            for record in reader:
                project_id = record['project_id']
                for key in record.keys():
                    value = record[key]
                    if value is None:
                        continue
                    if value.strip() == '':
                        continue
                    if key == 'project_id':
                        continue
                    upload_data.append({
                        'project_id': project_id,
                        'key': key.decode('utf-8').encode('utf-8'),
                        'value': value.decode('utf-8').encode('utf-8'),
                    })

                    if len(upload_data) >= self.bulk_size:
                        response = requester.upload_chunk(constants.PROJECT_METADATA_BULK_UPSERT_URL, upload_data)
                        if response.status_code != 200:
                            raise RuntimeError('Bad response attempting to upsert')
                        rows_loaded += len(upload_data)
                        logging.info("Loading {} rows, {} total so far".format(len(upload_data), rows_loaded))
                        upload_data = []

        # upload the leftovers
        if len(upload_data) > 0:
            response = requester.upload_chunk(constants.PROJECT_METADATA_BULK_UPSERT_URL, upload_data)
            if response.status_code != 200:
                raise RuntimeError('Bad response attempting to upsert')
            rows_loaded += len(upload_data)
        logging.info("{} metadata records loaded.".format(rows_loaded))


class LoadTraceCSVOperator(BaseOperator):
    ui_color = '#d1f7bb'

    @apply_defaults
    def __init__(self,
                 filename,
                 datastore_url,
                 access_token,
                 method='upsert',
                 bulk_size=1000,
                 *args, **kwargs):
        super(LoadTraceCSVOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.datastore_url = datastore_url
        self.access_token = access_token
        self.method = method
        self.bulk_size = bulk_size

    def execute(self, context):
        requester = Requester(self.datastore_url, self.access_token)

        try:
            data = pd.read_csv(self.filename, dtype=str).to_dict('records')
        except ValueError:
            return True

        unique_traces = list(set([
            (d["trace_id"], d["interpretation"], d["unit"]) for d in data
        ]))

        upload_data = []
        row_count = 0
        trace_pks_by_id = {}

        for trace in unique_traces:
            upload_data.append({
                "trace_id": trace[0],
                "interpretation": trace[1],
                "unit": trace[2],
                "added": datetime.utcnow().isoformat(),
                "updated": datetime.utcnow().isoformat(),
            })

            if len(upload_data) >= self.bulk_size / 2: # trace endpoint seems slow by comparison
                trace_response = requester.upload_chunk(constants.TRACE_BULK_UPSERT_VERBOSE_URL, upload_data)
                row_count += len(upload_data)

                if trace_response.status_code < 200 or trace_response.status_code >= 300:
                    raise RuntimeError('Bad response attempting to upsert traces')

                logging.info("Loaded {} trace rows, {} so far".format(len(upload_data), row_count))
                upload_data = []

                for record in trace_response.json():
                    trace_pks_by_id[record["trace_id"]] = record["id"]

        # leftovers
        if len(upload_data) > 0:
            trace_response = requester.upload_chunk(constants.TRACE_BULK_UPSERT_VERBOSE_URL, upload_data)
            row_count += len(upload_data)

            if trace_response.status_code < 200 or trace_response.status_code >= 300:
                raise RuntimeError('Bad response attempting to upsert traces')

            logging.info("Loaded {} trace rows, {} so far".format(len(upload_data), row_count))
            upload_data = []

            for record in trace_response.json():
                    trace_pks_by_id[record["trace_id"]] = record["id"]

        logging.info("Loaded {} trace rows".format(row_count))

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

        upload_data = []
        rows_loaded = 0
        for record in data:
            trace_record = {
                "trace_id": trace_pks_by_id[record["trace_id"]],
                "value": maybe_float(record["value"]),
                "start": record["start"],
                "estimated": record["estimated"],
            }

            upload_data.append(trace_record)
            if len(upload_data) >= self.bulk_size:
                if self.method == "upsert":
                    response = requester.upload_chunk(constants.TRACE_RECORD_BULK_UPSERT_URL, upload_data)
                elif self.method == "insert":
                    response = requester.upload_chunk(constants.TRACE_RECORD_BULK_INSERT_URL, upload_data)
                if response.status_code < 200 or response.status_code >=300:
                    raise RuntimeError('Bad response attempting to upsert')
                rows_loaded += len(upload_data)
                logging.info("Loaded {} trace records, {} so far".format(len(upload_data), rows_loaded))
                upload_data = []

        # load leftovers
        if len(upload_data) > 0:
            if self.method == "upsert":
                response = requester.upload_chunk(constants.TRACE_RECORD_BULK_UPSERT_URL, upload_data)
            elif self.method == "insert":
                response = requester.upload_chunk(constants.TRACE_RECORD_BULK_INSERT_URL, upload_data)
            if response.status_code < 200 or response.status_code >=300:
                raise RuntimeError('Bad response attempting to upsert')
            rows_loaded += len(upload_data)
            logging.info("Loaded {} trace records, {} so far".format(len(upload_data), rows_loaded))

        logging.info("Completed loading {} trace records".format(rows_loaded))


class LoadProjectTraceMapCSVOperator(BaseOperator):
    ui_color = '#d1f7bb'

    @apply_defaults
    def __init__(self,
                 filename,
                 datastore_url,
                 access_token,
                 bulk_size=1000,
                 *args, **kwargs):
        super(LoadProjectTraceMapCSVOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.datastore_url = datastore_url
        self.access_token = access_token
        self.bulk_size = bulk_size

    def execute(self, context):
        requester = Requester(self.datastore_url, self.access_token)

        response = requester.get(constants.PROJECT_ID_LIST_URL)
        loaded_project_ids = response.json()

        response = requester.get(constants.TRACE_ID_LIST_URL)
        loaded_trace_ids = response.json()

        trace_ids = {d["trace_id"]: d["id"] for d in loaded_trace_ids}
        project_ids = {d["project_id"]: d["id"] for d in loaded_project_ids}

        raw_matches = pd.read_csv(self.filename, dtype=str).to_dict('records')

        data = []
        row_count = 0
        for match in raw_matches:
            trace_id = trace_ids.get(match["trace_id"], None)
            project_id = project_ids.get(match["project_id"], None)

            if trace_id is not None and project_id is not None:
                data.append({
                    "trace_id": trace_id,
                    "project_id": project_id,
                })
                if len(data) >= self.bulk_size:
                    response = requester.upload_chunk(constants.PROJECT_TRACE_MAPPING_BULK_UPSERT_VERBOSE_URL, data)
                    if response.status_code < 200 or response.status_code >= 300:
                        raise RuntimeError('Bad response attempting to upsert')
                    row_count += len(data)
                    logging.info("Loaded {} proj-trace maps, {} so far".format(len(data), row_count))
                    data = []

        # leftovers
        if len(data) > 0:
            response = requester.upload_chunk(constants.PROJECT_TRACE_MAPPING_BULK_UPSERT_VERBOSE_URL, data)
            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError('Bad response attempting to upsert')
            row_count += len(data)
            logging.info("Loaded {} proj-trace maps, {} so far".format(len(data), row_count))

        logging.info("Completed loading {} proj-trace map records".format(row_count))


class AuditFormattedDataOperator(BaseOperator):
    ui_color = '#f2e0d7'

    @apply_defaults
    def __init__(self,
                 project_path,
                 trace_path,
                 mappings_path,
                 audit_results_file,
                 min_baseline_period_days=365,
                 min_reporting_period_days=365,
                 *args, **kwargs):
        super(AuditFormattedDataOperator, self).__init__(*args, **kwargs)
        self.project_path = project_path
        self.trace_path = trace_path
        self.mappings_path = mappings_path
        self.audit_results_file = audit_results_file
        self.min_baseline_period_days = min_baseline_period_days
        self.min_reporting_period_days = min_reporting_period_days

    def execute(self, context):
        task_output = {
            'project_files_read': [],
            'proj_trace_map_files_read': [],
            'trace_files_read': [],
            'projects_with_insufficient_baseline': [],
            'projects_without_trace': [],
            'projects_with_sufficient_baseline': [],
            'projects_with_insufficient_reporting_period': [],
            'projects_with_sufficient_reporting_period': [],
            'passing_projects': []
        }

        projects = {}

        logging.info("Reading projects...")
        proj_records = 0
        for filename in glob.glob(self.project_path):
            if not filename.endswith('.DS_Store'):
                with open(filename, 'r') as f_in:
                    reader = csv.DictReader(f_in)
                    for row in reader:
                        proj_records += 1
                        proj_id = row['project_id']
                        projects.setdefault(proj_id, {})
                        projects[proj_id]['baseline_period_end'] = row['baseline_period_end']
                        projects[proj_id]['reporting_period_start'] = row['reporting_period_start']
                task_output['project_files_read'].append(filename)
        logging.info("{} project records read.".format(proj_records))

        logging.info("Reading project-trace mappings...")
        trace_proj_map = {}
        proj_trace_records = 0
        for filename in glob.glob(self.mappings_path):
            if not filename.endswith('.DS_Store'):
                with open(filename, 'r') as f_in:
                    reader = csv.DictReader(f_in)
                    for row in reader:
                        proj_trace_records += 1
                        proj_id = row['project_id']
                        trace_id = row['trace_id']
                        trace_proj_map[trace_id] = proj_id
                task_output['proj_trace_map_files_read'].append(filename)
        logging.info("{} proj-trace records read.".format(proj_trace_records))

        logging.info("Reading traces...")
        traces = {}
        trace_records = 0
        for filename in glob.glob(self.trace_path):
            if not filename.endswith('.DS_Store'):
                with open(filename, 'r') as f_in:
                    reader = csv.DictReader(f_in)
                    for row in reader:
                        trace_records += 1
                        trace_id = row['trace_id']
                        traces.setdefault(trace_id, {})
                        usage_date = row['start']
                        if 'min_date' not in traces[trace_id]:
                            traces[trace_id]['min_date'] = usage_date
                        else:
                            current_min = traces[trace_id]['min_date']
                            d_usage_date = dateutil.parser.parse(usage_date)
                            d_current_min = dateutil.parser.parse(current_min)
                            if d_usage_date < d_current_min:
                                traces[trace_id]['min_date'] = usage_date

                        if 'max_date' not in traces[trace_id]:
                            traces[trace_id]['max_date'] = usage_date
                        else:
                            current_max = traces[trace_id]['max_date']
                            d_usage_date = dateutil.parser.parse(usage_date)
                            d_current_max = dateutil.parser.parse(current_max)
                            if d_usage_date > d_current_max:
                                traces[trace_id]['max_date'] = usage_date

                task_output['trace_files_read'].append(filename)

        logging.info("{} trace records read.".format(trace_records))

        logging.info("Determining min/max available usage dates for projects...")
        for trace in traces:
            proj_id = trace_proj_map[trace]
            min_usage_date = traces[trace]['min_date']
            max_usage_date = traces[trace]['max_date']
            if 'min_date' not in projects[proj_id]:
                projects[proj_id]['min_date'] = min_usage_date
            else:
                current_min = projects[proj_id]['min_date']
                d_usage_date = dateutil.parser.parse(min_usage_date)
                d_current_min = dateutil.parser.parse(current_min)
                if d_usage_date < d_current_min:
                    projects[proj_id]['min_date'] = min_usage_date

            if 'max_date' not in projects[proj_id]:
                projects[proj_id]['max_date'] = max_usage_date
            else:
                current_max = projects[proj_id]['max_date']
                d_usage_date = dateutil.parser.parse(max_usage_date)
                d_current_min = dateutil.parser.parse(current_max)
                if d_usage_date > d_current_max:
                    projects[proj_id]['max_date'] = max_usage_date

        logging.info("Checking sufficient baseline/reporting period...")
        for project in projects:
            sufficient_baseline = False
            sufficient_reporting_period = False

            if 'min_date' in projects[project]:
                baseline_period_end = projects[project]['baseline_period_end']
                min_usage = projects[project]['min_date']
                d_baseline_period_end = dateutil.parser.parse(baseline_period_end)
                d_min_usage = dateutil.parser.parse(min_usage)
                delta = d_baseline_period_end - d_min_usage
                if delta.days < self.min_baseline_period_days:
                    if project not in task_output['projects_with_insufficient_baseline']:
                        task_output['projects_with_insufficient_baseline'].append(project)
                else:
                    if project not in task_output['projects_with_sufficient_baseline']:
                        task_output['projects_with_sufficient_baseline'].append(project)
                        sufficient_baseline = True

            if 'max_date' in projects[project]:
                reporting_period_start = projects[project]['reporting_period_start']
                max_usage = projects[project]['max_date']
                d_reporting_period_start = dateutil.parser.parse(reporting_period_start)
                d_max_usage = dateutil.parser.parse(max_usage)
                delta = d_max_usage - d_reporting_period_start
                if delta.days < self.min_reporting_period_days:
                    if project not in task_output['projects_with_insufficient_reporting_period']:
                        task_output['projects_with_insufficient_reporting_period'].append(project)
                else:
                    if project not in task_output['projects_with_sufficient_reporting_period']:
                        task_output['projects_with_sufficient_reporting_period'].append(project)
                        sufficient_reporting_period = True

            if 'min_date' not in projects[project] and 'max_date' not in projects[project]:
                if project not in task_output['projects_without_trace']:
                    task_output['projects_without_trace'].append(project)

            if sufficient_baseline and sufficient_reporting_period:
                if project not in task_output['passing_projects']:
                    task_output['passing_projects'].append(project)

        task_output['projects'] = projects
        task_output['trace_proj_map'] = trace_proj_map
        task_output['traces'] = traces

        totals = {}
        for key in task_output:
            totals[key] = len(task_output[key])
            logging.info("{}: {}".format(key, totals[key]))
        task_output['totals'] = totals

        with open(self.audit_results_file, 'w') as f_out:
            f_out.write(json.dumps(task_output, indent=2))


class GoogleCloudStorageUploadOperator(BaseOperator):
    """
    Uploads a file to GCS
    """
    ui_color = '#42cbf4'

    @apply_defaults
    def __init__(self,
                 filename,
                 target,
                 bucket,
                 gcs_conn_id='google_cloud_storage_default',
                 *args,
                 **kwargs):
        super(GoogleCloudStorageUploadOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.target = target
        self.bucket = bucket
        self.gcs_conn_id = gcs_conn_id

    def execute(self, context):
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
        hook.upload(bucket=self.bucket, object=self.target, filename=self.filename)


class GoogleCloudStorageFileSensor(BaseSensorOperator):
    ui_color = '#18f4e9'

    @apply_defaults
    def __init__(self, 
                 bucket, 
                 object, 
                 gcs_conn_id='google_cloud_storage_default',
                 *args, 
                 **kwargs):
        super(GoogleCloudStorageFileSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.gcs_conn_id = gcs_conn_id

    def poke(self, context):
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
        logging.info('Poking: ' + self.object + ' in ' + self.bucket)
        service = hook.get_conn()
        try:
            service \
                .objects() \
                .get(bucket=self.bucket, object=self.object) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise


class CreateProjTraceMapFromJsonOperator(BaseOperator):
    """
    Creates OEE project-trace map CSV file from JSON map where keys
    are project_ids and values are trace_ids. Designed to work directly 
    with the style of dicts created by task_output_maps in
    TranslateCSVOperator:
    {
        "ProjectId": {
            "proj_id_1": ["trace_id_1", "trace_id_2"],
            "proj_id_2": ["trace_id_3", "trace_id_4"],
            ...
        }
    }
    """
    ui_color = '#f4f142'

    @apply_defaults
    def __init__(self,
                 in_file,
                 out_file,
                 map_name,
                 row_functions=[],
                 *args,
                 **kwargs):
        """
        """
        super(CreateProjTraceMapFromJsonOperator, self).__init__(*args, **kwargs)
        self.in_file = in_file
        self.out_file = out_file
        self.map_name = map_name
        self.row_functions = row_functions

    def execute(self, context):
        with open(self.in_file, 'r') as f_in:
            json_map = json.load(f_in)

        with open(self.out_file, 'w') as f_out:
            writer = csv.DictWriter(f_out, ['project_id', 'trace_id'])
            writer.writeheader()

            pt_maps = json_map[self.map_name]
            for key in pt_maps:
                rows = []
                for val in pt_maps[key]:
                    row = {'project_id': key, 'trace_id': val}
                    for func in self.row_functions:
                        row = func(row)
                    rows.append(row)
                writer.writerows(rows)



class TranslateCSVOperator(BaseOperator):
    """
    Performs generic CSV transforms
    """
    ui_color = '#f4bc42'

    @apply_defaults
    def __init__(self,
                 in_file,
                 out_file,
                 output_map_file=None,
                 field_map={},
                 field_value_map={},
                 extra_fields={},
                 task_output_maps={},
                 field_function_map={},
                 row_functions=[],
                 preprocess_functions=[],
                 skip_if_missing=False,
                 field_filters={},
                 *args,
                 **kwargs):
        """
        :param in_file: Path to incoming file to translate
        :type in_file: string
        :param out_file: Path to outgoing file for translation result
        :type out_file:string
        :param output_map_file: Path to write json formatted maps generated by
                                task_output_maps, if needed
        :type output_map_file: dict
        :param field_map: Dictionary of fields from in_file to map to out_file fields
        :type field_map: dict
        :param field_value_map: Dictionary of potential fields and their values to
                                replace with new values
        :type field_value_map: dict
        :param extra_fields: Dictionary of extra fields to add to out_file
        :type extra_fields: dict
        :param task_output_maps: Dictionary of extra maps to build while performing translation
        :type task_output_maps: dict
        :param field_function_map: Dictionary of fields mapped to functions to perform on them
        :type field_function_map: dict
        :param row_functions: List of functions to apply at the row level after all other
                              processing
        :type row_functions: list
        :param preprocess_functions: List of functions to apply at the row level before
                                     other processing
        :type preprocess_functions: list
        :param skip_if_missing: Flag to skip any rows that have blank/missing data in field_map
        :type skip_if_missing: bool
        :param field_filters: Dictionary of fields and values which, if encountered, should be skipped
        :type field_filters: dict
        """
        super(TranslateCSVOperator, self).__init__(*args, **kwargs)
        self.in_file = in_file
        self.out_file = out_file
        self.output_map_file = output_map_file
        self.field_map = field_map
        self.field_value_map = field_value_map
        self.extra_fields = extra_fields
        self.task_output_maps = task_output_maps
        self.field_function_map = field_function_map
        self.row_functions = row_functions
        self.preprocess_functions = preprocess_functions
        self.skip_if_missing = skip_if_missing
        self.field_filters = field_filters

    def execute(self, context):
        """
        Perform the translation in the following steps:
        1) run all preprocess_functions in order against rows
        2) apply filters in field_filters
        3) map fields to new row as is according to field_map
        4) create/append additional task output maps, if any
        5) skip the row if any fields in field_map are missing and skip_if_missing set
        6) add extra fields and their values to new translated row in extra_fields
        7) replace any new fields with values in field_value_map if any match
        8) apply field level functions to any new fields in field_function_map
        9) apply final row level functions in order of row_functions
        """
        task_output = {
            'skipped_records': []
        }

        in_file = self.in_file
        out_file = self.out_file
        output_map_file = self.output_map_file
        field_map = self.field_map
        field_value_map = self.field_value_map
        extra_fields = self.extra_fields
        task_output_maps = self.task_output_maps
        field_function_map = self.field_function_map
        row_functions = self.row_functions
        preprocess_functions = self.preprocess_functions
        skip_if_missing = self.skip_if_missing
        field_filters = self.field_filters

        with open(in_file, 'r') as f_in:
            reader = csv.DictReader(f_in, skipinitialspace=True)
            
            with open(out_file, 'w') as f_out:
                writer = None
                first_row = True
                for row in reader:
                    tx_row = {}

                    # Step 1 - run preprocessors
                    for func in preprocess_functions:
                        row = func(row)

                    # Step 1.5 - apply filters
                    skip_row = False
                    for key in row.keys():
                        if key in field_filters:
                            for val in field_filters[key]:
                                if row[key].strip() == val:
                                    skip_row = True
                    if skip_row:
                        continue

                    for key in row.keys():
                        # Step 2 - map fields in field_map as is
                        if key in field_map:
                            for fld in field_map[key]:
                                tx_row[fld] = row[key]

                        # Step 3 - create/append to output maps
                        if key in task_output_maps:
                            task_output.setdefault(key, {})
                            task_output[key].setdefault(row[key], [])
                            if row[task_output_maps[key]] not in task_output[key][row[key]]:
                                task_output[key][row[key]].append(row[task_output_maps[key]])

                    # Step 4 - if set to do so, skip the row if it is missing anything in the field_map
                    if skip_if_missing:
                        missing_field = False
                        for key in field_map:
                            if row[key] is None or row[key].strip() == '':
                                missing_field = True
                                task_output['skipped_records'].append(row)
                                break
                            if missing_field:
                                continue

                    # Step 5 - add extra fields
                    for key in extra_fields:
                        tx_row[key] = extra_fields[key]

                    # Step 6 - map field values per field_value_map
                    for key in field_value_map:
                        val_map = field_value_map[key]
                        val = tx_row[key].strip()
                        if val in val_map:
                            tx_row[key] = val_map[val]

                    # Step 7 - apply field level functions
                    for key in field_function_map:
                        for func in field_function_map[key]:
                            tx_row[key] = func(tx_row[key])

                    # Step 8 - apply row level functions
                    for func in row_functions:
                        tx_row = func(tx_row)

                    if first_row:
                        writer = csv.DictWriter(f_out, tx_row.keys())
                        writer.writeheader()
                        first_row = False

                    writer.writerow(tx_row)

        if output_map_file is not None:
            with open(output_map_file, 'w') as f:
                f.write(json.dumps(task_output))
