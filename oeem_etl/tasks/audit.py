import os
import json
import luigi
from tqdm import tqdm
import oeem_etl.config as config
from oeem_etl.tasks.upload import FetchFile
from oeem_etl.csvs import read_csv_file

class AuditFormattedData(luigi.Task):
    """Audit data after running formatting tasks
    """

    pattern = luigi.Parameter(default='', description="Only audit files that contain the string `pattern`")

    def requires(self):

        print("Gathering file paths")

        project_paths = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        trace_paths = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_TRACE_OUTPUT_DIR)

        def filter_paths(paths, apply_pattern=False):
            paths = [path for path in paths if not path.endswith('.DS_Store')]
            if self.pattern and apply_pattern:
                paths = [path for path in paths if self.pattern in path]
            return paths

        project_paths = filter_paths(project_paths)
        trace_paths = filter_paths(trace_paths, apply_pattern=True)

        return {
            'projects': [FetchFile(path) for path in project_paths],
            'traces': [FetchFile(path) for path in trace_paths],
            'mappings': FetchFile(config.oeem.full_path(config.oeem.OEEM_FORMAT_PROJECT_TRACE_MAPPING_PATH)),
        }

    def output(self):
        target = os.path.join(config.oeem.local_data_directory, 'audit_formatted_data.json')
        return config.oeem.target_class(target)

    def run(self):

        print("Gathering min/max dates of traces")

        mappings = read_csv_file(self.input()['mappings'].open('r'))
        # Assume one to one mapping
        project2trace = {row['project_id']: row['trace_id'] for row in mappings}

        # Find min/max dates in all traces
        min_dates = {}
        max_dates = {}

        audit_results = {
            'success': [],
            'no_trace': [],
            'min_date_gt_baseline_period_end': [],
            'max_date_lt_reporting_period_start': [],
            'total_success': 0,
            'total_failed': 0
        }

        file_count = 0
        for traces_file in tqdm(self.input()['traces']):
            file_count += 1
            traces = read_csv_file(traces_file.open('r'))

            for trace in traces:
                trace_id = trace['trace_id']
                if trace_id not in min_dates or min_dates[trace_id] > trace['start']:
                    min_dates[trace_id] = trace['start']
                if trace_id not in max_dates or max_dates[trace_id] < trace['start']:
                    max_dates[trace_id] = trace['start']

        # Compare min/max trace dates to project dates
        print("Testing inclusion status")

        inclusion_count = 0
        failed_inclusion_count = 0

        for projects_file in self.input()['projects']:
            projects = read_csv_file(projects_file.open('r'))
            for project in projects:
                project_id = project['project_id']
                trace_id = project2trace.get(project_id, None)
                if trace_id is None or trace_id not in min_dates:
                    failed_inclusion_count += 1
                    audit_results['no_trace'].append(project_id)
                elif min_dates[trace_id] > project['baseline_period_end']:
                    failed_inclusion_count += 1
                    audit_results['min_date_gt_baseline_period_end'].append(project_id)
                elif max_dates[trace_id] < project['reporting_period_start']:
                    failed_inclusion_count += 1
                    audit_results['max_date_lt_reporting_period_start'].append(project_id)
                else:
                    inclusion_count += 1
                    audit_results['success'].append(project_id)

        print("Failed inclusion test %s" % failed_inclusion_count)
        print("Succeeded inclusion test %s" % inclusion_count)

        audit_results['total_success'] = inclusion_count
        audit_results['total_failed'] = failed_inclusion_count

        with self.output().open('w') as f:
            f.write(json.dumps(audit_results))
