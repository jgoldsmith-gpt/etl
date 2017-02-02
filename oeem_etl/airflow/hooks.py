import logging
import httplib2
import base64
import tempfile
import os
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient import errors
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

logging.getLogger("google_cloud_storage").setLevel(logging.INFO)


class GCSHook(BaseHook):
    def __init__(self, conn_id, delegate_to=None):
        self.conn_id = conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(conn_id).extra_dejson

    def _authorize(self):
        base64key = self._get_field('base64key', False)
        if not base64key:
            base64key = os.getenv('GCS_CREDENTIALS', False)
            if base64key != False:
                logging.info('Using GCS_CREDENTIALS env var')
        else:
            logging.info('Using Airflow GCS connection info')

        scope = self._get_field('scope', 'https://www.googleapis.com/auth/devstorage.read_write')
        scopes = [s.strip() for s in scope.split(',')]

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not base64key:
            logging.info('Getting connection using gcloud auth user')
            credentials = GoogleCredentials.get_application_default()
        else:
            json_creds = base64.b64decode(base64key)
            fd, temp_path = tempfile.mkstemp()
            with open(temp_path, 'w') as key_file:
                key_file.write(json_creds)

            logging.info("Creds written to {}".format(temp_path))
            logging.info('Getting connection using JSON creds')
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                temp_path, scopes)
            http = httplib2.Http()
            auth = credentials.authorize(http)
            os.close(fd)
            os.remove(temp_path)

            return auth
        
    def _get_field(self, f, default=None):
        long_f = "{}".format(f)
        if long_f in self.extras:
            return self.extras[long_f]
        else:
            return default

    @property
    def project_id(self):
        return self._get_field('project')

    def get_conn(self):
        http_authorized = self._authorize()
        return build('storage', 'v1', http=http_authorized)

    def download(self, bucket, object, filename=False):
        service = self.get_conn()
        downloaded_file_bytes = service \
            .objects() \
            .get_media(bucket=bucket, object=object) \
            .execute()

        if filename:
            write_argument = 'wb' if isinstance(downloaded_file_bytes, bytes) else 'w'
            with open(filename, write_argument) as file_fd:
                file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes

    def upload(self, bucket, object, filename, mime_type='application/octet-stream'):
        service = self.get_conn()
        media = MediaFileUpload(filename, mime_type)
        response = service \
            .objects() \
            .insert(bucket=bucket, name=object, media_body=media) \
            .execute()

    def exists(self, bucket, object):
        service = self.get_conn()
        try:
            service \
                .objects() \
                .get(bucket=bucket, object=object) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    def is_updated_after(self, bucket, object, ts):
        service = self.get_conn()
        try:
            response = (service
                         .objects()
                         .get(bucket=bucket, object=object)
                         .execute())

            if 'updated' in response:
                import dateutil.parser
                import dateutil.tz

                if not ts.tzinfo:
                    ts = ts.replace(tzinfo=dateutil.tz.tzutc())

                updated = dateutil.parser.parse(response['updated'])
                logging.log(logging.INFO, "Verify object date: " + str(updated)
                            + " > " + str(ts))

                if updated > ts:
                    return True

        except errors.HttpError as ex:
            if ex.resp['status'] != '404':
                raise

        return False