import psycopg2
import pandas as pd
import os
from loguru import logger
import json
import glob
from cleaning_ops import immowelt_id_clean, address_clean, price_clean

host = 'localhost'
user = 'postgres'
dbname = 'postgres'
password = '091807'

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

ebay_path_raw = "Data/RawData/RawData_ebay/"
ebay_path_merged = "Data/MergedData/MergedData_ebay/"
ebay_path_clean = "Data/CleanData/"


directories_raw = os.listdir(ebay_path_raw)
directories_clean = os.listdir(ebay_path_clean)
keys_collection_clear = []


def log_documentation(result, situation, file_name, line):
    if True:
        if file_name is not None and line is not None:
            if result == 'e':
                logger.error('===ERROR===    The error was occurred while --- ' + situation +
                             ' in file' + str(file_name) +
                             ' and on the line ' + str(line))
            if result == 'd':
                logger.debug('The ' + situation +
                             ' in file ' + str(file_name) +
                             ' and on the line ' + str(line) + ' was successful')

        elif file_name is not None and line is None:
            if result == 'e':
                logger.error('===ERROR===    The error was occurred while --- ' + situation +
                             ' in file' + str(file_name))
            if result == 'd':
                logger.debug('The ' + situation +
                             ' in file ' + str(file_name) + ' was successful')

        else:
            if result == 'e':
                logger.error('The error was occurred while --- ' + situation)
            if result == 'd':
                logger.debug('The ' + situation + ' was successful')


def Table_ebay_crate():
    # collection of all keys
    global keys_collection_raw
    try:
        keys_collection_raw = []
        for file_name in range(len(directories_raw)):
            data_2 = pd.read_csv(ebay_path_raw + directories_raw[file_name])
            for key in data_2:
                keys_collection_raw.append(key)
    except:
        log_documentation('e', 'collection of all keys', None, None)
    else:
        log_documentation('d', 'collection of all keys', None, None)

    # deletion of repetiti  ons
    try:
        for item in keys_collection_raw:
            if item not in keys_collection_clear:
                keys_collection_clear.append(item)
    except:
        log_documentation('e', 'deletion of repetitions', None, None)
    else:
        log_documentation('d', 'deletion of repetitions', None, None)

    # creating a query to create a table
    try:
        table_craation_line = "CREATE TABLE ebay ("
        counter = 0
        for key in keys_collection_clear:
            counter += 1
            if counter == 1:
                table_craation_line += "Num VARCHAR, "
            elif (counter > 1) and (counter < len(keys_collection_clear)):
                table_craation_line += key + " VARCHAR, "
            elif counter == len(keys_collection_clear):
                table_craation_line += key + " VARCHAR);"
    except:
        log_documentation('e', 'creating a query to create a table', None, None)
    else:
        pass
        # log_documentation('d', 'creating a query to create a table', None, None)

    # enter a query to create a table
    try:
        cursor.execute(table_craation_line)
        conn.commit()
    except:
        log_documentation('e', 'enter a query to create a table', None, None)
    else:
        pass
        # log_documentation('d', 'enter a query to create a table', None, None)


def Table_ebay_copying():
    # example how to import csv and work in json

    files: list = glob.glob(ebay_path_raw + "*.csv")

    data_from_path: list = [pd.read_csv(file) for file in files]

    # assuming all dataframes have the same columns
    # columns: list = data_from_path[1].columns
    # big_dataframe: pd.DataFrame = pd.DataFrame(columns=columns)

    big_dataframe: pd.DataFrame = pd.concat(data_from_path, ignore_index=True)

    # turn to json

    data_json: json = big_dataframe.to_json()
    data_json = json.loads(data_json)
    with open(ebay_path_merged + "MergedData_ebay.json", "w") as f:
        json.dump(data_json, f, indent=4)


def Table_ebay_cleaning():
    # open the merged file and convert it to a dictionary
    new_merged_dict = {}
    clear_dict = {}
    with open(ebay_path_merged + "MergedData_ebay.json", "r") as f:
        data = json.load(f)

    # Creating a clean dict with another structure
    for i in data.keys():
        for line_num in data[i]:
            # logger.debug(line_num)
            dict_per_line_clear = {}
            dict_per_line_merged = {}
            for key in data.keys():
                if key == "immowelt_id":
                    dict_per_line_clear[key] = immowelt_id_clean(data[key][line_num])
                elif key == "address":
                    dict_per_line_clear[key] = address_clean(data[key][line_num])
                elif key == "price" or key == "num_rooms" or key == "space1" or key == "space3":
                    dict_per_line_clear[key] = price_clean(data[key][line_num])
                else:
                    dict_per_line_clear[key] = str(data[key][line_num]).replace(',', '_')
                dict_per_line_merged[key] = data[key][line_num]

            clear_dict[line_num] = dict_per_line_clear
            new_merged_dict[line_num] = dict_per_line_merged
        break

    # Inserting a clean dict into a new file
    f = open(ebay_path_clean + "CleanData_immowelt.json", "w")
    json.dump(clear_dict, f, indent=4)
    logger.info('CleanData_ebay was created')

    # Inserting a merged dict into a new file
    f = open(ebay_path_merged + "NewMergedData_immowelt.json", "w")
    json.dump(new_merged_dict, f, indent=4)
    logger.info('MergedData_ebay was created')


def Table_ebay_fill():
    with open(ebay_path_clean + "CleanData_immowelt.json", "r") as f:
        data = json.load(f)

    # creating the first part of the query to create the table
    try:
        table_fill_line_TableInfo = "INSERT INTO ebay ("
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
        log_documentation('e', 'creating the first part of the query to create the table', None, None)
    else:
        pass
        # log_documentation('d', 'creating the first part of the query to create the table', None, None)

    # creating an empty array with input data
    try:
        table_fill_line_DictInputData_old = {}
        for i in range(len(keys_collection_clear)):
            table_fill_line_DictInputData_old[keys_collection_clear[i]] = None
    except:
        log_documentation('e', 'creating an empty array with input data', None, None)
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

    # table_fill_line_Complate = table_fill_line_TableInfo
    # limiter = 0
    # for line_num in data.keys():
    #    for key in data[line_num]:
    #        limiter += 1
    #        if limiter != len(data[line_num]):
    #            table_fill_line_Complate += "'" + str((table_fill_line_DictInputData_n  ew[key])) + "',"
    #        elif limiter == len(data[line_num]):
    #            pass
    #        logger.debug(data[line_num][key])

        # logger.info('The file "' + directories[file_number] + '" was successful filled in the table "immowelt"')
    logger.info('The table "immowelt" was filled')
