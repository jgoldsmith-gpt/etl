import airflow
from airflow.models import Connection
from airflow import settings

session = settings.Session()

# check existing connections
connections = session.query(Connection).filter(Connection.conn_id == 'google_cloud_storage_default')
if connections.count() > 0:
    print("Default GCS connection already configured.")
else:
    print("Setting up GCS default connection")
    connection = Connection()
    connection.conn_id = 'google_cloud_storage_default'
    connection.conn_type = 'google_cloud_storage'
    session.add(connection)
    session.commit()
    print("Complete")

session.close()
