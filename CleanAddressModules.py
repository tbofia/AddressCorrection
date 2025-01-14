import logging
import sqlalchemy
import pandas as pd
from datetime import datetime
import pyodbc
from geopy.geocoders import GoogleV3
import requests
import os
import urllib.parse
import urllib3


# This function returns as database engine
def get_db_connection(server, database):
    # Set up the SQL Server connection
    connectionstring = "mssql+pyodbc://{}/{}?driver=ODBC+Driver+17+for+SQL+Server"
    connectionstring = connectionstring.format(server, database)

    try:
        connection = sqlalchemy.create_engine(connectionstring, fast_executemany=True)
    except Exception as e:
        logging.error("An exception occurred while creating database engine: %s", e)
        connection = None

    return connection


# This function returns list of addresses to be processed from Database
def get_dim_addresses(connection, config_attribs, last_load_cut_off):
    select_statement = ("SELECT TOP {} {} "
                        "      ,ADDRESS_LINE_1 "
                        "      ,ADDRESS_LINE_2 "
                        "      ,ATTENTION AS NOTES"
                        "      ,CITY "
                        "      ,STATE "
                        "      ,ZIP "
                        "      ,COUNTY "
                        "FROM {}.{} "
                        "WHERE dss_record_source like '%Plandata.dbo%' "
                        "AND dss_current_flag = 'Y' "
                        "AND LTRIM(RTRIM(ADDRESS_LINE_1)) NOT LIKE '%PO BOX%' "
                        "AND {} > {} "
                        "ORDER BY {}"
                        .format(config_attribs['number_of_addresses'],
                                config_attribs['source_table_key'],
                                config_attribs['source_schema'],
                                config_attribs['source_table'],
                                config_attribs['source_table_key'],
                                last_load_cut_off,
                                config_attribs['source_table_key']
                                ))
    try:
        address_list_df = pd.read_sql(select_statement, connection)
        return [address_list_df, "Success"]
    except Exception as e:
        print("Could Not Read Address List from Database")
        return [None, e]


# This function will check if an address has already been cleaned
def check_if_address_has_been_processed(address_key, config_attribs, connection):
    conn = connection.connect()
    address_exists = None
    try:
        address_exists = conn.execute(
            sqlalchemy.text(
                "SELECT COUNT(1) AS TotalRecords "
                "FROM {}.{} "
                "WHERE {}=:src_address_key ".format(
                    config_attribs['target_schema'],
                    config_attribs['target_table'],
                    config_attribs['target_table_key']
                )
            ),
            {"src_address_key": address_key},
        ).fetchone()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(
            "An exception occurred while checking if record exists. error log: %s",
            e,
        )
    return address_exists[0] > 0


def get_last_loaded_record_key(config_attribs, connection):
    conn = connection.connect()
    last_load_cut_off = 0
    try:
        last_load_cut_off = conn.execute(
            sqlalchemy.text(
                "SELECT ISNULL(MAX({}),0) as load_key_cut_off "
                "FROM {}.{} ".format(
                    config_attribs['target_table_key'],
                    config_attribs['target_schema'],
                    config_attribs['target_table']
                )
            ),
        ).fetchone()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(
            "An exception occurred while checking latest loaded record. error log: %s",
            e,
        )
    return last_load_cut_off[0]


def get_address_components(address):
    address_components = {'street_number': '', 'street_name': '', 'city': '', 'state': '', 'zipcode': '', 'county': '',
                          'country': '', 'subpremise': ''}

    for component in address:
        if 'street_number' in component['types']:
            address_components['street_number'] = component['long_name']
        elif 'route' in component['types']:
            address_components['street_name'] = component['short_name'].upper()
        elif 'locality' in component['types']:
            address_components['city'] = component['long_name'].upper()
        elif 'postal_code' in component['types']:
            address_components['zipcode'] = component['long_name']
        elif 'administrative_area_level_2' in component['types']:
            address_components['county'] = component['short_name'].upper()
        elif 'administrative_area_level_1' in component['types']:
            address_components['state'] = component['short_name'].upper()
        elif 'country' in component['types']:
            address_components['country'] = component['short_name'].upper()
        elif 'subpremise' in component['types']:
            address_components['subpremise'] = '#' + component['long_name'].upper()

    return address_components


# This function saves address returned from cleanup into database table
def save_usps_address(address_key, original_address_elements, response_result, config_attribs, connection):
    # Let's get the county using City and State
    address_components = get_address_components(response_result[0]['address_components'])

    address_entry = {
        "responsedate": datetime.now(),
        "responsemessage": response_result[1],
        "responseaccuracy": response_result[0]['geometry']['location_type'],
        "responsetypes": (".").join(response_result[0]['types']),
        "responsematchcount": response_result[2],
        "NOTES": original_address_elements.NOTES.strip(),
        "SOURCE_SYSTEM_KEY": address_key,
        "SOURCE_SYSTEM_ADDRESS_LINE_1": original_address_elements.ADDRESS_LINE_1.strip(),
        "SOURCE_SYSTEM_ADDRESS_LINE_2": original_address_elements.ADDRESS_LINE_2.strip(),
        "SOURCE_SYSTEM_CITY": original_address_elements.CITY.strip(),
        "SOURCE_SYSTEM_STATE": original_address_elements.STATE.strip(),
        "SOURCE_SYSTEM_COUNTY": original_address_elements.COUNTY.strip(),
        "SOURCE_SYSTEM_ZIP": original_address_elements.ZIP.strip(),
        "ADDRESS_LINE_1": (' ').join([address_components['street_number'], address_components['street_name']]).strip(),
        "ADDRESS_LINE_2": address_components['subpremise'],
        "CITY": address_components['city'],
        "STATE": address_components['state'],
        "COUNTY": address_components['county'],
        "ZIP": address_components['zipcode'],
        "COUNTRY": address_components['country'],
        "LATITUDE": response_result[0]["geometry"]["location"]['lat'],
        "LONGITUDE": response_result[0]["geometry"]["location"]["lng"]
    }

    try:
        # Add Error log entry
        pd.DataFrame([address_entry]).to_sql(
            name=config_attribs['target_table'],
            con=connection,
            schema=config_attribs['target_schema'],
            if_exists="append",
            index=False,
        )
        return True
    except Exception as e:
        logging.error("An exception occurred while logging error: %s", e)
        return False


def save_error_requests(address_key, original_address_elements, response_result, config_attribs, connection):
    address_entry = {
        "responsedate": datetime.now(),
        "responsemessage": response_result[1],
        "responseaccuracy": 'NA',
        "responsetypes": 'NA',
        "responsematchcount": 0,
        "NOTES": original_address_elements.NOTES.strip(),
        "SOURCE_SYSTEM_KEY": address_key,
        "SOURCE_SYSTEM_ADDRESS_LINE_1": original_address_elements.ADDRESS_LINE_1.strip(),
        "SOURCE_SYSTEM_ADDRESS_LINE_2": original_address_elements.ADDRESS_LINE_2.strip(),
        "SOURCE_SYSTEM_CITY": original_address_elements.CITY.strip(),
        "SOURCE_SYSTEM_STATE": original_address_elements.STATE.strip(),
        "SOURCE_SYSTEM_COUNTY": original_address_elements.COUNTY.strip(),
        "SOURCE_SYSTEM_ZIP": original_address_elements.ZIP.strip(),
        "LATITUDE": -1,
        "LONGITUDE": -1
    }

    try:
        # Add Error log entry
        pd.DataFrame([address_entry]).to_sql(
            name=config_attribs['target_table'],
            con=connection,
            schema=config_attribs['target_schema'],
            if_exists="append",
            index=False,
        )
        return True
    except Exception as e:
        logging.error("An exception occurred while logging error: %s", e)
        return False


# This function returns the county for a given address
def call_address_api_requests(address, config_attribs):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    google_api_key = os.getenv('GOOGLE_API_KEY', config_attribs['api_key'])

    encoded_address = urllib.parse.quote(address)
    url = f"{config_attribs['api_url']}address={encoded_address}&key={google_api_key}"

    try:
        response = requests.get(url, verify=False).json()

        if response['status'] == 'OK':
            response_result = response['results'][0]
            return [response_result, response['status'], len(response['results'])]
        else:
            return [None, response['status'], 0]
    except Exception as e:
        return [None, 'Failed Request', "No Address Information: {}".format(e)]


# This function calls api to clean up an address
def call_address_api_geopy(address, config_attribs):
    geolocator = GoogleV3(api_key=config_attribs['api_key'])

    try:
        location = geolocator.geocode(address, exactly_one=True)
        return [location.raw, 'OK', 1]
    except Exception as e:
        return [None, 'Failed Request', "No Address Information: {}".format(e)]


# This function takes a dataframe row and returns a string concatenation of address elements
def create_address_string(address_elements):
    address_elements_list = [address_elements.ADDRESS_LINE_1.strip(),
                             address_elements.ADDRESS_LINE_2.strip(),
                             address_elements.CITY.strip(),
                             address_elements.STATE.strip(),
                             address_elements.ZIP.strip()]

    # remove empty elements from the list
    address_elements_list[:] = [item for item in address_elements_list if item != '']
    address_string = ",".join(address_elements_list)

    return address_string


# This function cleans and saves addresses will be called in thread
def clean_and_save_address(address, config_attribs, connection):
    address_string = create_address_string(address)

    if not check_if_address_has_been_processed(address.EDW_DIM_ADDRESS_KEY, config_attribs, connection):
        response_result = call_address_api_requests(address_string, config_attribs)

        if response_result[1] == 'OK':
            if save_usps_address(address.EDW_DIM_ADDRESS_KEY,
                                 address,
                                 response_result,
                                 config_attribs,
                                 connection):
                return "Success"
            else:
                return "Failure: Could not Save clean Address."
        else:
            save_error_requests(address.EDW_DIM_ADDRESS_KEY, address, response_result, config_attribs, connection)
            return "Failure: Did not retrieve clean Address.", response_result[1]
    else:
        return "Success: Address has already been validated against USPS."
