import psycopg2
import pandas as pd
import os
from loguru import logger
import json
import glob
from cleaning_ops import *

host = 'localhost'
user = 'postgres'
dbname = 'postgres'
password = '091807'

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()


def log_documentation(result, process):
    if result == 'e':
        logger.error('The error was occurred while --- ' + process)
    if result == 'd':
        logger.debug('The ' + process + ' was successful')


def Table_crate(table_name):
    global keys_collection_clear, keys_collection_raw
    path_raw = "Data/RawData/RawData_" + table_name + "/"
    directories_raw = os.listdir(path_raw)

    # collection of all keys
    try:
        keys_collection_raw = []
        for file_name in range(len(directories_raw)):
            data = pd.read_csv(path_raw + directories_raw[file_name])
            for key in data:
                keys_collection_raw.append(key)
    except:
        log_documentation('e', 'collection of all keys')
    else:
        log_documentation('d', 'collection of all keys')

    # deletion of repetitions
    try:
        keys_collection_clear = []
        for item in keys_collection_raw:
            if item not in keys_collection_clear:
                keys_collection_clear.append(item)
    except:
        log_documentation('e', 'deletion of repetitions')
    else:
        log_documentation('d', 'deletion of repetitions')

    # creating a query to create a table
    try:
        table_creation_line = "CREATE TABLE " + table_name + " ("
        counter = 0
        for key in keys_collection_clear:
            counter += 1
            if counter == 1:
                table_creation_line += "Num VARCHAR, "
            elif (counter > 1) and (counter < len(keys_collection_clear)):
                table_creation_line += key + " VARCHAR, "
            elif counter == len(keys_collection_clear):
                table_creation_line += key + " VARCHAR);"
    except:
        log_documentation('e', 'creating a query to create a table')
    else:
        log_documentation('d', 'creating a query to create a table')

    # enter a query to create a table
    try:
        cursor.execute(table_creation_line)
        conn.commit()
    except:
        log_documentation('e', 'enter a query to create a table')
    else:
        log_documentation('d', 'enter a query to create a table')

    logger.info('The table "' + table_name + '" was created')


def Table_copying(table_name):
    path_raw = "Data/RawData/RawData_" + table_name + "/"
    path_merged = "Data/MergedData/MergedData_" + table_name + "/"

    # example how to import csv and work in json
    try:
        files: list = glob.glob(path_raw + "*.csv")
        data_from_path: list = [pd.read_csv(file) for file in files]
        big_dataframe: pd.DataFrame = pd.concat(data_from_path, ignore_index=True)
    except:
        log_documentation('e', 'example how to import csv and work in json')
    else:
        log_documentation('d', 'example how to import csv and work in json')
    # turn to json
    try:
        data_json: json = big_dataframe.to_json()
        data_json = json.loads(data_json)
        with open(path_merged + "MergedData_" + table_name + ".json", "w") as f:
            json.dump(data_json, f, indent=4)
    except:
        log_documentation('e', 'turn to json')
    else:
        log_documentation('d', 'turn to json')
    logger.info('The data for table "' + table_name + '" was copied')


def Table_cleaning(table_name):
    path_merged = "Data/MergedData/MergedData_" + table_name + "/"
    path_clean = "Data/CleanData/"

    # open the merged file and convert it to a dictionary
    new_merged_dict = {}
    clear_dict = {}
    with open(path_merged + "MergedData_" + table_name + ".json", "r") as f:
        data = json.load(f)

    # Creating a clean dict with another structure
    for i in data.keys():
        for line_num in data[i]:
            dict_per_line_clear = {}
            dict_per_line_merged = {}
            for key in data.keys():
                if key == "immowelt_id":
                    dict_per_line_clear[key] = immowelt_id_clean(data[key][line_num])
                elif key == "address":
                    dict_per_line_clear[key] = address_clean(data[key][line_num])
                elif key == "price" or key == "num_rooms" or key == "space" or key == "space1" or key == "space3":
                    dict_per_line_clear[key] = price_clean(data[key][line_num])
                elif key == "immonet_id":
                    dict_per_line_clear[key] = immonet_id_clean(data[key][line_num])
                elif key == "seller_id":
                    dict_per_line_clear[key] = seller_id_clean(data[key][line_num])
                else:
                    dict_per_line_clear[key] = str(data[key][line_num]).replace(',', '_')
                dict_per_line_merged[key] = data[key][line_num]

            clear_dict[line_num] = dict_per_line_clear
            new_merged_dict[line_num] = dict_per_line_merged
        break

    # Inserting a clean dict into a new file
    f = open(path_clean + "CleanData_" + table_name + ".json", "w")
    json.dump(clear_dict, f, indent=4)
    logger.debug('CleanData_' + table_name + ' was created')

    # Inserting a merged dict into a new file
    f = open(path_merged + "NewMergedData_" + table_name + ".json", "w")
    json.dump(new_merged_dict, f, indent=4)
    logger.debug('MergedData_' + table_name + ' was created')


def Table_fill(table_name):
    path_clean = "Data/CleanData/"

    with open(path_clean + "CleanData_" + table_name + ".json", "r") as f:
        data = json.load(f)

    # creating the first part of the query to create the table
    try:
        table_fill_line_TableInfo = "INSERT INTO " + table_name + " ("
        counter = 0
        for i in range(len(keys_collection_clear)):
            counter += 1
            if counter == 1:
                table_fill_line_TableInfo += "num, "
            elif counter > 1 and (counter < len(keys_collection_clear)):
                table_fill_line_TableInfo += str(keys_collection_clear[i]) + ", "
            elif counter == len(keys_collection_clear):
                table_fill_line_TableInfo += keys_collection_clear[i] + ") VALUES ("
    except:
        log_documentation('e', 'creating the first part of the query to create the table')
    else:
        pass
        # log_documentation('d', 'creating the first part of the query to create the table', None, None)

    # creating an empty array with input data
    try:
        table_fill_line_DictInputData_old = {}
        for i in range(len(keys_collection_clear)):
            table_fill_line_DictInputData_old[keys_collection_clear[i]] = None
    except:
        log_documentation('e', 'creating an empty array with input data')
    else:
        pass
        # log_documentation('d', 'creating an empty array with input data', None, None)

    # entering data into a database row
    for line_num in data.keys():
        table_fill_line_Complete = table_fill_line_TableInfo
        limiter = 0
        for key in data[line_num]:
            limiter += 1
            if limiter != len(data[line_num]):
                table_fill_line_Complete += "'" + str(data[line_num][key]).replace("'", '_').replace(',', ' ') + "',"
            else:
                table_fill_line_Complete += "'" + str(data[line_num][key]).replace("'", '_').replace(',', ' ') + "');"
        cursor.execute(table_fill_line_Complete)
        conn.commit()
    logger.info('The table "' + table_name + '" was filled')
