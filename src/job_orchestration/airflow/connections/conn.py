from airflow import settings
from airflow.models.connection import Connection
import sqlalchemy

conn = Connection(
    conn_id="spark_standalone",
    conn_type="spark",
    description="Spark standalone connection",
    host="spark://localhost:7077"
)

session = settings.Session()
try:
    session.add(conn)
    session.commit()
    session.close()
except sqlalchemy.exc.IntegrityError:
    print('Airflow connections already added')