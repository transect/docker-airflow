from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import logging
import os
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['datateam@transect.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=20),
}

dag = DAG(
    'site_selection',
    default_args=default_args,
    description='Parcel Assessment',
    schedule_interval="@once",
    start_date=datetime(2021, 4, 1),
    tags=['parcels']
)


aoi = {'state': 'ak', 'county': 'haines'}
remote_zip = f"{aoi['state']}_{aoi['county']}.gpkg.zip"
local_zip = f"{aoi['state']}_{aoi['county']}.zip"
local_file = f"/usr/local/airflow/{aoi['state']}_{aoi['county']}.gpkg"
playground_postgres_host = Variable.get('playground-postgres-host')
playground_postgres_password = Variable.get('playground-postgres-password')
load_bash = f'ogr2ogr "PG:dbname=postgres ' \
            f'user=postgres ' \
            f'host={playground_postgres_host} ' \
            f'password={playground_postgres_password}" ' \
            f'-t_srs EPSG:4326 -select owner,county,mailadd,mail_zip,wkb_geometry {local_file} ' \
            f'-nln parcels_raw ' \
            f'-lco GEOMETRY_NAME=geometry'


def extract_file():
    filepath = f"/usr/local/airflow/{local_zip}"
    with ZipFile(filepath, 'r') as zip_file:
        zip_file.extractall()


def delete_files():
    os.remove(local_zip)
    os.remove(local_file)
    return 'Zip and csv deleted'


def clean_fields():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    clean_up_query = """
        update parcels_raw
        set mail_zip = substring(mail_zip,1,5),
          mailadd = upper(mailadd),
          "owner" = upper("owner");
    """
    pg_hook.run(clean_up_query)


def dissolve_parcels():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    dissolve_mail_query = """
        insert into parcels (county, "owner", mailadd, mailzip, geometry)
        select max(county), max("owner"), mailadd, mail_zip, ST_Union(geometry) 
        from parcels_raw
        where mailadd is not null and mail_zip is not null
        group by mailadd, mail_zip;
    """
    dissolve_owner_query = """
        insert into parcels (county, "owner", mailadd, mailzip, geometry)
        select max(county), "owner", max(mailadd), max(mail_zip), ST_Union(geometry) 
        from parcels_raw
        where (mailadd is null or mail_zip is null) and "owner" is not null
        group by "owner";
    """
    no_dissolve_query = """
        insert into parcels (county, "owner", mailadd, mailzip, geometry)
        select county, "owner", mailadd, mail_zip, geometry
        from parcels_raw
        where mailadd is null and mail_zip is null and "owner" is null
    """
    pg_hook.run(dissolve_mail_query)
    pg_hook.run(dissolve_owner_query)
    pg_hook.run(no_dissolve_query)


def filter_parcels():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    calculate_acreage_query = """
        update parcels
        set gisacre = round(st_area(geometry::geography)*0.000247105, 2)
        where gisacre is null;
    """
    remove_tails_query = """
        delete from parcels
        where gisacre < 25 or gisacre > 2500
    """
    pg_hook.run(calculate_acreage_query)
    pg_hook.run(remove_tails_query)


get_file = SFTPOperator(
    task_id='get_file',
    ssh_conn_id='landgrid',
    local_filepath=f"/usr/local/airflow/{local_zip}",
    remote_filepath=f"/download/geoPKG/{remote_zip}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

extract_file = PythonOperator(
    task_id='extract_file',
    python_callable=extract_file,
    dag=dag
)

load_parcels = BashOperator(
    task_id='load_parcels',
    bash_command=load_bash,
    dag=dag,
)


get_file >> extract_file >> load_parcels
