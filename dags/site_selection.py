from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import logging
import os
import requests
import shutil
from zipfile import ZipFile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['datateam@transect.com'],
    'email_on_failure': False,
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

aoi = {'state': 'tx', 'county': 'comal'}
remote_zip = f"{aoi['state']}_{aoi['county']}.gpkg.zip"
local_zip = f"{aoi['state']}_{aoi['county']}.zip"
local_file = f"/usr/local/airflow/{aoi['state']}_{aoi['county']}.gpkg"
playground_postgres_host = Variable.get('playground-postgres-host')
playground_postgres_password = Variable.get('playground-postgres-password')
playground_postgres_db = Variable.get('playground-postgres-db')
playground_postgres_user = Variable.get('playground-postgres-user')

load_parcels_bash = f'ogr2ogr "PG:dbname={playground_postgres_db} ' \
            f'user={playground_postgres_user} ' \
            f'host={playground_postgres_host} ' \
            f'password={playground_postgres_password}" ' \
            f'-t_srs EPSG:4326 -select owner,county,mailadd,mail_zip,wkb_geometry {local_file} ' \
            f'-nln parcels_raw ' \
            f'-lco GEOMETRY_NAME=geometry'
build_vrt_bash = f'gdalbuildvrt /usr/local/airflow/mosaic.vrt /usr/local/airflow/*.tif'
mosaic_bash = f'gdal_translate -of GTiff -co "TILED=YES" /usr/local/airflow/mosaic.vrt /usr/local/airflow/mosaic.tif'
calculate_slope_bash = f'gdaldem slope /usr/local/airflow/mosaic.tif /usr/local/airflow/slope.tif -p -s 111120 ' \
                       f'-alg Horn -compute_edges -co "TILED=YES"'
reproject_bash = f'gdalwarp -t_srs EPSG:4326 /usr/local/airflow/slope.tif /usr/local/airflow/slope_4326.tif'
load_slope_bash = f'raster2pgsql -I -C -M -F -t 100x100 /usr/local/airflow/slope_4326.tif slope' \
                  f' | PGPASSWORD={playground_postgres_password} psql -d {playground_postgres_db} ' \
                  f'-U {playground_postgres_user} -h {playground_postgres_host}'
remove_tif_bash = f'rm /usr/local/airflow/*.tif /usr/local/airflow/*.mvt'


def extract_file():
    filepath = f"/usr/local/airflow/{local_zip}"
    with ZipFile(filepath, 'r') as zip_file:
        zip_file.extractall()


def prepare_parcel_tables():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    drop_query = """
        drop table if exists parcels_raw
    """
    create_query = """
        create table parcels
        as table parcels_template
        with no data;
    """
    add_sequence_query = """
        alter table parcels add column id serial;
    """
    add_spatial_index_query = """
        create index "parcels_geometry_idx"
        on parcels using gist ("geometry");
    """
    pg_hook.run(drop_query)
    pg_hook.run(create_query)
    pg_hook.run(add_sequence_query)
    pg_hook.run(add_spatial_index_query)


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


def calculate_acreage():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    calculate_acreage_query = """
        update parcels
        set gisacre = st_area(geometry::geography)*0.000247105
        where gisacre is null;
    """
    pg_hook.run(calculate_acreage_query)


def filter_parcels():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    remove_tails_query = """
        delete from parcels
        where gisacre < 25 or gisacre > 2500
    """
    pg_hook.run(remove_tails_query)


def calculate_proximity_line():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    distance_to_line_query = """
        with candidate_match as (
        select g1.id as gref_gid,
               g2.id as gnn_gid
        from parcels as g1
        join lateral (
          select id
          from transmission_lines as g
          where g1.line_distance is null
          order by g1.geometry <-> g.geometry
          limit 20
        ) as g2
        on true),
        nearest_distance as (
        select min(st_distance(geography(g1.geometry), geography(g2.geometry)))*0.000621371 as d,
               g1.id
        from candidate_match, parcels g1, transmission_lines g2
        where g1.id = gref_gid and g2.id = gnn_gid
        group by g1.id)

        update parcels
        set line_distance = nearest_distance.d
        from nearest_distance
        where parcels.id = nearest_distance.id
    """
    pg_hook.run(distance_to_line_query)


def calculate_proximity_substation():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    distance_to_substation_query = """
        with candidate_match as (
        select g1.id as gref_gid,
               g2.id as gnn_gid
        from parcels as g1
        join lateral (
          select id
          from substations as g
          where g1.substation_distance is null
          order by g1.geometry <-> g.geometry
          limit 20
        ) as g2
        on true),
        nearest_distance as (
        select min(st_distance(geography(g1.geometry), geography(g2.geometry)))*0.000621371 as d,
               g1.id
        from candidate_match, parcels g1, transmission_lines g2
        where g1.id = gref_gid and g2.id = gnn_gid
        group by g1.id)

        update parcels
        set substation_distance = nearest_distance.d
        from nearest_distance
        where parcels.id = nearest_distance.id
    """
    pg_hook.run(distance_to_substation_query)


def get_raster_index():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    get_index_query = f"""
        select array_agg(tile_index_1deg.product_id)
        from tile_index_1deg, counties
        where counties.state = '{aoi['state']}' and counties.name ilike '{aoi['county'] + ' county'}'
          and st_intersects(tile_index_1deg.geometry, counties.geometry);
    """
    file_index = pg_hook.get_records(get_index_query)[0][0]
    return file_index


def get_dem(**context):
    base_url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1/TIFF/'
    id_tuple = context['task_instance'].xcom_pull(task_ids='get_raster_index')
    for product_id in id_tuple:
        url = f'{base_url}{product_id}/USGS_1_{product_id}.tif'
        local_filename = url.split('/')[-1]
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)


def calculate_median_slope():
    pg_hook = PostgresHook(postgres_conn_id='playground_database')
    calculate_median_slope_query = """
        update parcels
        set median_slope = slope.median_slope
        from (
          select ST_Quantile(st_union(st_clip(rast,1,geometry,True)), 0.5) as median_slope, parcels.id
          from slope_raster, parcels
          where st_intersects(rast, geometry)
          group by parcels.id 
        ) as slope
        where parcels.id = slope.id
        ;
    """
    pg_hook.run(calculate_median_slope_query)

# get_file = SFTPOperator(
#     task_id='get_file',
#     ssh_conn_id='landgrid',
#     local_filepath=f"/usr/local/airflow/{local_zip}",
#     remote_filepath=f"/download/geoPKG/{remote_zip}",
#     operation="get",
#     create_intermediate_dirs=True,
#     dag=dag
# )
#
# extract_file = PythonOperator(
#     task_id='extract_file',
#     python_callable=extract_file,
#     dag=dag
# )
#
# prepare_parcel_tables = PythonOperator(
#     task_id='prepare_parcel_tables',
#     python_callable=prepare_parcel_tables,
#     dag=dag
# )
#
# load_parcels_bash = BashOperator(
#     task_id='load_parcels',
#     bash_command=load_parcels_bash,
#     dag=dag,
# )
#
# clean_fields = PythonOperator(
#     task_id='clean_fields',
#     python_callable=clean_fields,
#     dag=dag
# )
#
# dissolve_parcels = PythonOperator(
#     task_id='dissolve_parcels',
#     python_callable=dissolve_parcels,
#     dag=dag
# )
#
# delete_files = PythonOperator(
#     task_id='delete_files',
#     python_callable=delete_files,
#     dag=dag
# )
#
# calculate_acreage = PythonOperator(
#     task_id='calculate_acreage',
#     python_callable=calculate_acreage,
#     dag=dag
# )
#
# filter_parcels = PythonOperator(
#     task_id='filter_parcels',
#     python_callable=filter_parcels,
#     dag=dag
# )
#
# calculate_proximity_line = PythonOperator(
#     task_id='calculate_proximity_line',
#     python_callable=calculate_proximity_line,
#     dag=dag
# )
#
# calculate_proximity_substation = PythonOperator(
#     task_id='calculate_proximity_substation',
#     python_callable=calculate_proximity_substation,
#     dag=dag
# )


get_raster_index = PythonOperator(
    task_id='get_raster_index',
    python_callable=get_raster_index,
    dag=dag
)

get_dem = PythonOperator(
    task_id='get_dem',
    python_callable=get_dem,
    provide_context=True,
    dag=dag
)
#
# build_vrt = BashOperator(
#     task_id='build_vrt',
#     bash_command=build_vrt_bash,
#     dag=dag,
# )
#
# mosaic_dem = BashOperator(
#     task_id='mosaic_dem',
#     bash_command=mosaic_bash,
#     dag=dag,
# )
#
# calculate_slope = BashOperator(
#     task_id='calculate_slope',
#     bash_command=calculate_slope_bash,
#     dag=dag,
# )
#
# load_slope = BashOperator(
#     task_id='load_slope',
#     bash_command=load_slope_bash,
#     dag=dag
# )
#
# remove_tif = BashOperator(
#     task_id='remove_tif',
#     bash_command=remove_tif_bash,
#     dag=dag
# )
#
# calculate_median_slope = PythonOperator(
#     task_id='calculate_median_slope',
#     python_callable=calculate_median_slope,
#     dag=dag
# )

# get_file >> [extract_file, prepare_parcel_tables] >> load_parcels >> clean_fields >> [dissolve_parcels, delete_files]
# dissolve_parcels >> calculate_acreage >> filter_parcels >> calculate_proximity_line >> calculate_proximity_substation
# get_dem >> build_vrt_bash >> mosaic_bash >> load_slope >> [remove_tif, calculate_median_slope]
get_raster_index >> get_dem
