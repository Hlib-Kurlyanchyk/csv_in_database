import psycopg2
import pandas as pd
import os
from loguru import logger
import json
import glob

host = 'localhost'
user = 'postgres'
dbname = 'postgres'
password = '091807'

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

immowelt_path_raw = "Data/RawData/RawData_immowelt/"
immowelt_path_clean = "Data/CleanData/CleanData_immowelt/"
immowelt_path_merged = "Data/MergedData/"

directories_raw = os.listdir(immowelt_path_raw)
directories_clean = os.listdir(immowelt_path_clean)
keys_collection_clear = []

# print(len(data_2.keys()))
# print(len(data_2))


def LOG_DOCUMENTATION(result, situation, file_name, line):
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


def TABLE_IMMOWELT_DATA_COPYING():
    big_dict = {}
    #for file_name in directories_raw:
        #file_name_nocsv = str(file_name[:len(file_name)-4])
        #try:
        #    df = pd.read_csv(r'' + immowelt_path_raw + file_name)
        #    df.to_json(r'' + immowelt_path_clean + file_name_nocsv + '.json')
        #    big_dict = {**big_dict, **df}
        #except:
        #    LOG_DOCUMENTATION('e', 'data copying in json', file_name_nocsv, None)
        #else:
        #    LOG_DOCUMENTATION('d', 'data copying in json', file_name_nocsv, None)

    # example how to import csv and work in json

    logger.debug(glob.glob(immowelt_path_raw + "*.csv"))
    files: list = glob.glob(immowelt_path_raw + "*.csv")

    data_from_path: list = [pd.read_csv(file) for file in files]

    # assuming all dataframes have the same columns
    # columns: list = data_from_path[1].columns
    # big_dataframe: pd.DataFrame = pd.DataFrame(columns=columns)

    big_dataframe: pd.DataFrame = pd.concat(data_from_path, ignore_index=True)
    print(big_dataframe.info())

    # turn to json

    data_json: json = big_dataframe.to_json()
    with open(immowelt_path_merged + "MergedData_immowlet.json", "w") as f:
        json.dump(data_json, f, indent=2)

    #with open(immowelt_path_merged + "MergedData_immowlet.json", "w") as file:
    #    stock = json.load(file)

    #with open(immowelt_path_merged + "MergedData_immowlet.json", "w") as file:
    #    json.dump(stock, file, indent=4)


    #list_houses = []
    #for iter in range(len(big_dict)):
    #    list_houses.append({"headline": big_dict["headline"][iter]})
    #    list_houses.append({"price": big_dict["price"][iter]})

    #list_houses.to_json(r'' + immowelt_path_clean + file_name_nocsv + '.json')

    #with open(immowelt_path_clean + 'BIG.json', "w") as file:
    #    json.dump(list_houses, file, indent=4)


def TABLE_IMMOWELT_CREATE():
    # collection of all keys
    try:
        keys_collection_raw = []
        for file_name in range(len(directories_raw)):
            data_2 = pd.read_csv(immowelt_path_raw + directories_raw[file_name])
            for key in data_2:
                keys_collection_raw.append(key)
    except:
        LOG_DOCUMENTATION('e', 'collection of all keys', None, None)
    else:
        LOG_DOCUMENTATION('d', 'collection of all keys', None, None)

    # deletion of repetiti  ons
    try:
        for item in keys_collection_raw:
            if item not in keys_collection_clear:
                keys_collection_clear.append(item)
    except:
        LOG_DOCUMENTATION('e', 'deletion of repetitions', None, None)
    else:
        LOG_DOCUMENTATION('d', 'deletion of repetitions', None, None)

    # creating a query to create a table
    try:
        table_craattion_line = "CREATE TABLE immowelt ("
        counter = 0
        for key in keys_collection_clear:
            counter += 1
            if counter == 1:
                table_craattion_line += "ID VARCHAR, "
            elif (counter > 1) and (counter < len(keys_collection_clear)):
                table_craattion_line += key + " VARCHAR, "
            elif counter == len(keys_collection_clear):
                table_craattion_line += key + " VARCHAR);"
    except:
        LOG_DOCUMENTATION('e', 'creating a query to create a table', None, None)
    else:
        pass
        # LOG_DOCUMENTATION('d', 'creating a query to create a table', None, None)

    # enter a query to create a table
    try:
        cursor.execute(table_craattion_line)
        conn.commit()
    except:
        LOG_DOCUMENTATION('e', 'enter a query to create a table', None, None)
    else:
        pass
        # LOG_DOCUMENTATION('d', 'enter a query to create a table', None, None)


def TABLE_IMMOWELT_FILL(border):
    # creating the first part of the query to create the table
    try:
        table_fill_line_TableInfo = "INSERT INTO immowelt ("
        counter = 0
        for i in range(len(keys_collection_clear)):
            counter += 1
            if counter == 1:
                table_fill_line_TableInfo += "ID, "
            elif counter > 1 and (counter < len(keys_collection_clear)):
                table_fill_line_TableInfo += str(keys_collection_clear[i]) + ", "
            elif counter == len(keys_collection_clear):
                table_fill_line_TableInfo += keys_collection_clear[i] + ") VALUES ("
    except:
        LOG_DOCUMENTATION('e', 'creating the first part of the query to create the table', None, None)
    else:
        pass
        # LOG_DOCUMENTATION('d', 'creating the first part of the query to create the table', None, None)

    # creating an empty array with input data
    try:
        table_fill_line_DictInputData_old = {}
        for i in range(len(keys_collection_clear)):
            table_fill_line_DictInputData_old[keys_collection_clear[i]] = None
    except:
        LOG_DOCUMENTATION('e', 'creating an empty array with input data', None, None)
    else:
        pass
        # LOG_DOCUMENTATION('d', 'creating an empty array with input data', None, None)

    # ///
    for file_name in directories_raw:  # work with each file
        data_2 = pd.read_csv(immowelt_path_raw + file_name)
        if border < 1:
            border = len(data_2)  # the bound is the number of rows to be processed
        for i in range(border):  # work with each line in a file
            table_fill_line_DictInputData_new = table_fill_line_DictInputData_old
            try:
                for k in range(len(keys_collection_clear)):  # work with each key in a line
                    if k == 17:
                        print()
                    # filling an array with input data
                    for key in data_2:  # checking each key
                        if key == keys_collection_clear[k]:
                            table_fill_line_DictInputData_new[key] = \
                                str(data_2[key][i]).replace('"', '-').replace("'", "-")
            except:
                LOG_DOCUMENTATION('e', 'filling an array with input data', file_name, i)
            else:
                pass
                # LOG_DOCUMENTATION('d', 'filling an array with input data', file_number, i)

            # creating a complete input string
            table_fill_line_Complate = table_fill_line_TableInfo
            counter = 0
            try:
                for key in table_fill_line_DictInputData_new:
                    counter += 1
                    if counter != len(table_fill_line_DictInputData_new):
                        # print(key)
                        # print("    " + table_fill_line_DictInputData_new[key])
                        # print()

                        if key == 'immowelt_id':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        elif key == 'price':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        elif key == 'address':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        elif key == 'num_rooms':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        elif key == 'space1':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        elif key == 'space3':
                            table_fill_line_Complate += \
                                "'" + str((table_fill_line_DictInputData_new[key])) + "',"

                        else:
                            table_fill_line_Complate += "'" + str(table_fill_line_DictInputData_new[key]) + "',"
                    else:
                        table_fill_line_Complate += "'" + str(table_fill_line_DictInputData_new[key]) + "');"
            except:
                LOG_DOCUMENTATION('e', 'creating a complete input string', file_name, i)
            else:
                pass
                # LOG_DOCUMENTATION('d', 'creating a complete input string', file_number, i)

            # enter the full input string
            try:
                # logger.debug(table_fill_line_Complate)
                cursor.execute(table_fill_line_Complate)
                conn.commit()
            except:
                LOG_DOCUMENTATION('e', 'enter the full input string', file_name, i)
            else:
                pass
                LOG_DOCUMENTATION('d', 'enter the full input string', file_name, i)

        # logger.info('The file "' + directories[file_number] + '" was successful filled in the table "immowelt"')
    logger.info('The table "immowelt" was filled')
