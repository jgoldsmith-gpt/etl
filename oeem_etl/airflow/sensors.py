from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from oeem_etl.airflow.hooks import GCSHook
from googleapiclient import errors
import logging


class GCSFileSensor(BaseSensorOperator):
    """
    Checks GCS for presence of a file
    """
    ui_color = '#18f4e9'

    @apply_defaults
    def __init__(self, 
                 bucket, 
                 object, 
                 gcs_conn_id='GOOGLE_CLOUD_STORAGE_DEFAULT',
                 *args, 
                 **kwargs):
        """
        :param bucket: GCS bucket name to check
        :type bucket: string
        :param object: GCS object to check
        :type object: string
        :param gcs_conn_id: Name of Airflow connection id to use
        :type gcs_conn_id: string
        """
        super(GCSFileSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.gcs_conn_id = gcs_conn_id

    def poke(self, context):
        hook = GCSHook(conn_id=self.gcs_conn_id)
        logging.info('Poking: ' + self.object + ' in ' + self.bucket)
        return hook.exists(self.bucket, self.object)
