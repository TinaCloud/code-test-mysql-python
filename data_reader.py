import pymysql.cursors
import argparse
from pprint import pprint as pp


def main(city):
    # Connect to the database
    connection = pymysql.connect(host='173.194.106.251',
                                 user='root',
                                 password='InfectiousTraining',
                                 db='test_rui',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    sql = """SELECT a.City_name 'City'
            , a.City_id 'City id'
            , a.City_iso_code 'City ISO code'
            , b.Region_name 'Region'
            , a.Region_id 'Region id'
            , b.Region_iso_code 'Region ISO code'
            , c.Country_name 'Country'
            , c.Country_id 'Country id'
            , c.Country_iso2_code 'Country ISO2 code'
            , c.Country_iso_code 'Country ISO code'
            , CASE c.Targetable
                WHEN 1 THEN 'yes'
                ELSE 'no'
               END 'Is the country targetable'
        FROM test_rui.Cities a
        JOIN test_rui.Regions b
            ON a.Region_id = b.Region_id
        JOIN test_rui.Countries c
            ON a.Country_id = c.Country_id
        WHERE upper(City_name) = upper('{0}');""".format(city)

    cursor.execute(sql)
    result = cursor.fetchall()

    pp(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Retrieves all data for a specific city')

    parser.add_argument('city',
                            help='name of the city (not case sensitive)',
                            type=str)


    args = parser.parse_args()

    main(args.city)
