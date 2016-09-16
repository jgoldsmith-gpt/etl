import luigi
import oeem_etl.config as config
from oeem_etl.tasks.upload2 import FetchFile
from oeem_etl.csvs import read_csv_file

class AuditFormattedData(luigi.Task):
    """Audit data after running formatting tasks
    """

    pattern = luigi.Parameter(default='', description="Only audit files that contain the string `pattern`")

    def requires(self):

        print("Gathering file paths")

        project_paths     = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        consumption_paths = config.oeem.storage.get_existing_paths(config.oeem.OEEM_FORMAT_CONSUMPTIONS_OUTPUT_DIR)

        def filter_paths(paths, apply_pattern=False):
            paths = [path for path in paths if not path.endswith('.DS_Store')]
            if self.pattern and apply_pattern:
                paths = [path for path in paths if self.pattern in path]
            return paths

        project_paths = filter_paths(project_paths)
        consumption_paths = filter_paths(consumption_paths, apply_pattern=True)

        return {
            'projects': [FetchFile(path) for path in project_paths],
            'consumptions': [FetchFile(path) for path in consumption_paths]
        }

    def run(self):

        print("Gathering min/max dates of consumption traces")

        # Find min/max dates in all consumption traces
        min_dates = {}
        max_dates = {}

        file_count = 0
        for consumptions_file in self.input()['consumptions']:
            file_count += 1
            print(file_count)
            consumptions = read_csv_file(consumptions_file.open('r'))

            for consumption in consumptions:
                project_id = consumption['project_id']
                if project_id not in min_dates or min_dates[project_id] > consumption['start']:
                    min_dates[project_id] = consumption['start']
                if project_id not in max_dates or max_dates[project_id] < consumption['start']:
                    max_dates[project_id] = consumption['start']

        # Compare min/max consumption trace dates to project dates
        print("Testing inclusion status")

        inclusion_count = 0
        failed_inclusion_count = 0

        for projects_file in self.input()['projects']:
            projects = read_csv_file(projects_file.open('r'))

            for project in projects:
                project_id = project['project_id']
                if project_id not in min_dates:
                    failed_inclusion_count += 1
                elif min_dates[project_id] > project['baseline_period_end']:
                    failed_inclusion_count += 1
                elif max_dates[project_id] < project['reporting_period_start']:
                    failed_inclusion_count += 1
                else:
                    inclusion_count += 1

        print("Failed inclusion test %s" % failed_inclusion_count)
        print("Succeeded inclusion test %s" % inclusion_count)