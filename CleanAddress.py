import concurrent.futures
import configparser
import CleanAddressModules
import pandas as pd

if __name__ == "__main__":
    # get information from configuration file.
    config = configparser.RawConfigParser()
    config.read(".config")

    config_params = {
        "targetserver": config["DATABASE_SERVER"]["TARGETSERVER"],
        "targetdatabase": config["DATABASE_SERVER"]["TARGETDATABASE"],
        "sourceserver": config["DATABASE_SERVER"]["SOURCESERVER"],
        "sourcedatabase": config["DATABASE_SERVER"]["SOURCEDATABASE"],
        "api_key": config["MISC"]["API_KEY"],
        "api_url": config["MISC"]["API_URL"],
        "max_threads": int(config["MISC"]["NUMBER_OF_THREADS"]),
        "number_of_addresses": int(config["MISC"]["NUMBER_OF_ADDRESSES"]),
        "source_schema": config["DATABASE_SERVER"]["SOURCESCHEMA"],
        "source_table": config["DATABASE_SERVER"]["SOURCETABLE"],
        "source_table_key": config["DATABASE_SERVER"]["SOURCETABLEKEY"],
        "target_schema": config["DATABASE_SERVER"]["TARGETSCHEMA"],
        "target_table": config["DATABASE_SERVER"]["TARGETTABLE"],
        "target_table_key": config["DATABASE_SERVER"]["TARGETTABLEKEY"]
    }

    src_conn = CleanAddressModules.get_db_connection(config_params["sourceserver"], config_params["sourcedatabase"])
    dst_conn = CleanAddressModules.get_db_connection(config_params["targetserver"], config_params["targetdatabase"])

    # Get List of addresses to process as a Dataframe
    last_load_cut_off = CleanAddressModules.get_last_loaded_record_key(config_params, dst_conn)
    address_list = CleanAddressModules.get_dim_addresses(src_conn, config_params, last_load_cut_off)

    if isinstance(address_list[0], pd.DataFrame):
        # Create a ThreadPoolExecutor with the desired number of threads
        with concurrent.futures.ThreadPoolExecutor(config_params['max_threads']) as executor:

            for address in address_list[0].itertuples():
                future = executor.submit(CleanAddressModules.clean_and_save_address
                                         , address
                                         , config_params
                                         , dst_conn)
    else:
        print('Could Not get addresses from database: ', address_list[1])





