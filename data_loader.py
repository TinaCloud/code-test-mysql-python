import pymysql.cursors
import logging
import gzip
from os import path
import pandas as pd
from tabulate import tabulate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
'''
FUTURE IMPROVEMENT
output logging to a log file as well as the console

improve error catching for different types of errors
'''


def check_object_exits(active_cursor, type, schema, object):
    if type == 'table':
        sql = """SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = '{0}' AND TABLE_NAME = '{1}';""".format(schema, object)
    elif type == 'constraint':
        sql = """SELECT COUNT(*)
                    FROM information_schema.table_constraints
                    WHERE table_schema = '{0}' AND CONSTRAINT_NAME = '{1}';""".format(schema, object)

    active_cursor.execute(sql)
    if active_cursor.fetchone()['COUNT(*)'] == 1:
        return True
    else:
        return False


def check_user_exits(active_cursor, user_name):
    sql = """SELECT COUNT(*) FROM mysql.user WHERE User = '{0}';""".format(user_name)
    active_cursor.execute(sql)

    if active_cursor.fetchone()['COUNT(*)'] == 1:
        return True
    else:
        return False


def get_table_count(active_cursor, table):
    sql = """SELECT COUNT(*) {0} FROM test_rui.{0};""".format(table)
    active_cursor.execute(sql)
    return active_cursor.fetchone()[table]


def main():

    files_path = '/Users/rui/Desktop/IMTest/data'

    # Connect to the database
    connection = pymysql.connect(host='173.194.106.251',
                                 user='root',
                                 password='InfectiousTraining',
                                 db='test_rui',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    '''
    FUTURE IMPROVEMENTS
    files path and database connection parameters should be moved to an external config file
    like a yaml file or similar

    Queries were not working without specifying the database/schema but ideally this should be removed from
    the queries to make it easier to point to different environments

    Files should be downloaded programmatically once I figure out how to authenticate to GCS

    Address the warning displayed when using the dataframe method to_sql before it gets deprecated
    FutureWarning: The 'mysql' flavor with DBAPI connection is deprecated and will be removed in future versions.
    MySQL will be further supported with SQLAlchemy connectables.
    chunksize=chunksize, dtype=dtype)

    Ideally the tables shouldn't need to be created before but by using the pandas.to_csv option where a new table is
    created was creating errors at the time of adding foreign and primary keys. The dtype parameter could solve the
    problem but I only found it too late.
    '''

    if check_object_exits(cursor, 'table', 'test_rui', 'Countries') == False:
        logger.info('Creating table Countries...')
        sql = """CREATE TABLE test_rui.Countries (
                    Country_id INT(11) NOT NULL,
                    Country_iso2_code VARCHAR(2),
                    Country_iso_code VARCHAR(3),
                    Country_name VARCHAR(100),
                    Targetable TINYINT(1),
                PRIMARY KEY (Country_id));"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    if check_object_exits(cursor, 'table', 'test_rui', 'Regions') == False:
        logger.info('Creating table Regions...')
        sql = """CREATE TABLE test_rui.Regions (
                    Region_id INT NOT NULL,
                    Country_id INT NOT NULL,
                    Region_name VARCHAR(100) NOT NULL,
                    Region_iso_code VARCHAR(3) NOT NULL,
                  PRIMARY KEY (Region_id));"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    if check_object_exits(cursor, 'table', 'test_rui', 'Cities') == False:
        logger.info('Creating table Cities...')
        sql = """CREATE TABLE test_rui.Cities (
                      City_id INT NOT NULL,
                      Country_id INT,
                      Region_id INT,
                      City_name VARCHAR(100),
                      City_iso_code VARCHAR(3),
                  PRIMARY KEY (City_id));"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    if check_object_exits(cursor, 'constraint', 'test_rui', 'fk_Regions_Country_id') == False:
        logger.info('Adding Country_id constraint to Regions table...')
        sql = """ALTER TABLE test_rui.Regions
                    ADD CONSTRAINT fk_Regions_Country_id
                    FOREIGN KEY (Country_id)
                    REFERENCES test_rui.Countries (Country_id)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION;"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    if check_object_exits(cursor, 'constraint', 'test_rui', 'fk_Cities_Region_id') == False:
        logger.info('Adding Region_id constraint to Cities table...')
        sql = """ALTER TABLE test_rui.Cities
                    ADD CONSTRAINT fk_Cities_Region_id
                    FOREIGN KEY (Region_id)
                    REFERENCES test_rui.Regions (Region_id)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION;"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    if check_object_exits(cursor, 'constraint', 'test_rui', 'fk_Cities_Country_id') == False:
        logger.info('Adding Country_id constraint to Cities table...')
        sql = """ALTER TABLE test_rui.Cities
                    ADD CONSTRAINT fk_Cities_Country_id
                    FOREIGN KEY (Country_id)
                    REFERENCES test_rui.Countries (Country_id)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION;"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)

    if check_user_exits(cursor, 'data_reader') == False:
        logger.info('Creating user data_reader@localhost')
        sql = """CREATE USER data_reader@localhost IDENTIFIED BY 'data_reader';"""
        try:
            cursor.execute(sql)
        except Exception as e:
            logger.error(e)


    logger.info('Granting permissions to user data_reader')
    sql = """GRANT SELECT ON test_rui.Countries TO data_reader@localhost;
             GRANT SELECT ON test_rui.Regions TO data_reader@localhost;
             GRANT SELECT ON test_rui.Cities TO data_reader@localhost;
             FLUSH PRIVILEGES;"""
    try:
        cursor.execute(sql)
    except Exception as e:
        logger.error(e)

    logger.info('Processing countries file...')
    countries_file = path.join(files_path, 'countries.gzip')
    if not path.exists(countries_file):
        logger.error('Countries file missing from {0}'.format(files_path))
        sys.exit(0)
    with gzip.open(countries_file, 'rb') as f:
        countries_dataframe = pd.read_csv(f,  sep=',', quotechar='"', index_col=0,encoding='utf-8', header=0,
                                      names=['Country_id', 'Country_iso2_code', 'Country_iso_code', 'Country_name', 'Targetable'])
    countries_dataframe.to_sql(con=connection, name='Countries', if_exists='append', flavor='mysql')


    logger.info('Processing regions file')
    regions_file = path.join(files_path, 'regions.csv')
    if not path.exists(regions_file):
        logger.error('regions file missing from {0}'.format(files_path))
        sys.exit(0)
    regions_dataframe = pd.read_csv(regions_file,  sep=',', index_col=0,encoding='utf-8', header=0,
                                    names=['Region_id', 'Country_id', 'Region_name', 'Region_iso_code'])
    regions_dataframe.to_sql(con=connection, name='Regions', if_exists='append', flavor='mysql')


    logger.info('Processing cities file')
    cities_file = path.join(files_path, 'cities.gz')
    if not path.exists(cities_file):
        logging.error('cities file missing from {0}'.format(files_path))
        sys.exit(0)

    with gzip.open(cities_file, 'rb') as f:
        cities_data = [l.decode('utf-8') for l in f.readlines()]

    cities_data_json = '[' + ','.join(cities_data) + ']'
    cities_dataframe = pd.read_json(cities_data_json, typ='frame', dtype=False)
    cities_dataframe.columns = ['Country_id', 'City_id', 'City_iso_code', 'City_name', 'Region_id']
    cities_dataframe.set_index('City_id', inplace=True)
    cities_dataframe.to_sql(con=connection, name='Cities', if_exists='append', flavor='mysql')


    '''
    FUTURE IMPROVEMENT
    the checking imports section could be replaced by some metadata tables where file names, counts, etc
    could be recorded in a persistent way and be used to derive metrics and linage
    '''

    logger.info('Checking imports...')
    table_counts = {
        'Cities': get_table_count(cursor, 'Cities'),
        'Regions': get_table_count(cursor, 'Regions'),
        'Countries': get_table_count(cursor, 'Countries')}

    data_quality_summary = []
    data_quality_summary.append([countries_file, 'Countries', len(countries_dataframe.index),
                                 table_counts['Countries'], len(countries_dataframe.index) - table_counts['Countries']])
    data_quality_summary.append([regions_file,' Regions', len(regions_dataframe.index),
                                 table_counts['Regions'], len(regions_dataframe.index) - table_counts['Regions']])
    data_quality_summary.append([cities_file, 'Cities', len(cities_dataframe.index),
                                 table_counts['Cities'], len(cities_dataframe.index) - table_counts['Cities']])

    logger.info("\n" + tabulate(data_quality_summary, headers=['File name','Output table','Rows in file',
                                                               'Rows in table', 'Missing rows']))

    '''
    FUTURE IMPROVEMENT
     notify by email data engineering team, unless the scheduling system already cover that functionality
    '''

if __name__ == '__main__':
    main()
