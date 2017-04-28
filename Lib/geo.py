import urllib
import requests
from requests_oauthlib import OAuth1
import json


def latlong_from_yahoo(address):
    """
    Get Latitude longitude from address
    :param address:
    :return:
    """
    # keys
    client_key = 'dj0yJmk9ZUtvYVZYaURqNVNGJmQ9WVdrOVQzcEpVRlZ3TXpnbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD03NQ'
    client_secret = '59bfb1cbf4272ad5656a10b5660697781ba202b9'

    # Using OAuth1 to make a simple query request on when your application doesn't require any permissions
    baseurl = "https://query.yahooapis.com/v1/public/yql?"
    yql_query = "select centroid from geo.places where text=\"" + address + "\""
    yql_url = baseurl + urllib.urlencode({'q': yql_query}) + "&format=json"
    queryoauth = OAuth1(client_key, client_secret, signature_type='query')
    r = requests.get(url=yql_url, auth=queryoauth)

    result = json.loads(r.content.decode('utf8'))
    latitude = 0
    longitude = 0

    if result['query']['count'] > 1:
        latitude = result['query']['results']['place'][0]['centroid']['latitude']
        longitude = result['query']['results']['place'][0]['centroid']['longitude']
    elif result['query']['count'] == 1:
        latitude = result['query']['results']['place']['centroid']['latitude']
        longitude = result['query']['results']['place']['centroid']['longitude']
    elif result['query']['count'] == 0:
        print "nO resut"

    return latitude, longitude


def insert_address_row_sqlite(c, housestreet, cityprovince, postalcode):
    """
    Insert address to sqlite datanase
    :param c: Cursor for sqlite connection
    :param housestreet:
    :param cityprovince:
    :param postalcode:
    :return:
    """
    # print(housestreet, cityprovince, postalcode)
    sql_text = """
        INSERT OR REPLACE INTO AddressMerged
        (IsUpdated,HouseStreet,CityProvince,PostalCode,Latitude,Longitude)
        VALUES (MAX(COALESCE((SELECT IsUpdated FROM AddressMerged
                    WHERE HouseStreet = ?
                    AND CityProvince= ?
                    AND PostalCode = ?),0), 0),?,?,?,
        COALESCE((SELECT Latitude FROM AddressMerged
                    WHERE HouseStreet = ?
                    AND CityProvince= ?
                    AND postalcode = ?), NULL),
        COALESCE((SELECT Longitude FROM AddressMerged
                    WHERE HouseStreet = ?
                    AND CityProvince= ?
                    AND postalcode = ?), NULL)
        );"""

    c.execute(sql_text, (housestreet, cityprovince, postalcode,
                         housestreet, cityprovince, postalcode,
                         housestreet, cityprovince, postalcode,
                         housestreet, cityprovince, postalcode))


def update_latlong_sqllite(c, latitude, longitude, housestreet,
                           cityprovince, postalcode):
    """
    :param c: cursor for sql update
    :param latitude:
    :param longitude:
    :param housestreet:
    :param cityprovince:
    :param postalcode:
    :return:
    """
    sql_insert_template = """
        UPDATE AddressMerged
          SET Latitude=?, Longitude=?, IsUpdated=1
          WHERE HouseStreet= ?
            AND CityProvince= ?
            AND PostalCode =?
        """

    c.execute(sql_insert_template, (latitude, longitude,
                                    housestreet, cityprovince,
                                    postalcode))


def find_latlong(c, housestreet, cityprovince, postalcode):
    """
    Get Latitude and longitude
    :param c:
    :param housestreet:
    :param cityprovince:
    :param postalcode:
    :return:
    """
    latitude = 0
    longitude = 0

    sql_text = """
    SELECT Latitude, Longitude
    FROM AddressMerged
      WHERE HouseStreet=?
      AND CityProvince =?
      AND PostalCode = ? LIMIT 1"""
    # print sql_text
    c.execute(sql_text, (housestreet, cityprovince, postalcode))

    csvline = c.fetchall()
    if len(csvline) > 0:
        latitude, longitude = csvline[0]
    # print latitude, longitude
    return latitude, longitude
