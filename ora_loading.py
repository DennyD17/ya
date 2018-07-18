# coding: utf8

import cx_Oracle
import random
from string import ascii_uppercase, ascii_lowercase, digits
import csv
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import logging
from datetime import datetime
import argparse
import time


def argument_parser():
    """
    parse arguments from command line
    :return: filename of file to create, tablename to insert, filename of file with data
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-random_csv', help="Name of csv file if need to create it and set random values.")
    parser.add_argument('-table', help="Name of the table to insert data from csv file.")
    parser.add_argument('-csv', help="Name of existing csv file with data")
    args = parser.parse_args()
    if (args.random_csv and args.csv) or (not args.random_csv and not args.csv):
        parser.error("Use csv parameter or random_csv parameter. For more info run help (-h)")
    return args.random_csv, args.table, args.csv


def get_logger(filename):
    """
    Create logger with passed filename
    """
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(filename)
    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def generate_random_row(num_rows):
    """
    :param num_rows: number of rows
    :return: row
    """
    for _ in range(num_rows):
        yield [random.randint(0, 1000000),
               ''.join(random.choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(10)),
               ''.join(random.choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(10)),
               random.randint(0, 10000000)
               ]


def write_to_csv(file_name, num_of_rows):
    """
    Writing random rows to csv
    :param file_name: output csv file
    :param num_of_rows: number of rows to insert in csv file
    :return: None
    """
    with open(file_name, 'w') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        for row in generate_random_row(num_of_rows):
            try:
                writer.writerow(row)
            except Exception as e:
                logger.error("Cant, write row to csv")
                logger.debug(str(e))


def get_ora_settings(config_file='settings.ini'):
    """
    Getting ora settings from settings.ini file
    :param config_file: path to .ini file
    :return: connection string to ora db
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    try:
        host = config.get('ora', 'host')
        port = config.get('ora', 'port')
        username = config.get('ora', 'username')
        password = config.get('ora', 'password')
        sid = config.get('ora', 'sid')
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.critical('Wrong info in settings.py file: ' + str(e))
        raise Exception('Wrong info in settings.py file: ' + str(e))
    else:
        return username + '/' + password + '@' + host + ':' + port + '/' + sid


def make_batch_insert(cursor, rows, tablename):
    """
    Make batch insert into table in db
    :param rows: rows to insert
    :return: number of errors getting while inserting rows
    """
    cursor.prepare("INSERT INTO %s (ID, TEXT_FIELD_1, TEXT_FIELD_2, num_field) VALUES (:1, :2, :3, :4)" % tablename)
    cursor.executemany(None, rows, batcherrors=True)
    errors = cursor.getbatcherrors()
    if errors:
        for error in errors:
            message = error.message
            error_query = str(rows[error.offset])
            logger.error("Cant insert row %s in table %s" % (error_query, tablename))
            logger.debug(message)
    return len(errors)


def insert_rows_to_table(csv_filename, tablename):
    """
    Main function to insert data from csv file to table
    :param csv_filename: filename of csv file with data
    :param tablename: name of the table to insert data from csv
    :return: number of errors getting while inserting
    """
    conn = cx_Oracle.connect(get_ora_settings())
    cursor = conn.cursor()
    cursor.bindarraysize = 1000
    rows_to_insert = []
    errors_counter = 0
    with open(csv_filename) as f:
        for idx, row in enumerate(csv.reader(f)):
            rows_to_insert.append(row)
            if idx % 1000 != 0 or idx == 0:
                continue
            else:
                errors_counter += make_batch_insert(cursor, rows_to_insert, tablename)
                conn.commit()
                rows_to_insert = []
        if len(rows_to_insert) != 0:
            errors_counter += make_batch_insert(cursor, rows_to_insert, tablename)
            conn.commit()
    conn.close()
    return errors_counter


if __name__ == '__main__':
    random_csv, tablename, csv_name = argument_parser()
    filename = random_csv if not csv_name else csv_name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    logger = get_logger('from_' + filename + '_to_' + tablename + '_' + timestamp + '.log')
    if random_csv:
        write_to_csv(filename, 100000)
    start = time.time()
    errors = insert_rows_to_table(filename, tablename)
    t = time.time() - start
    logger.info('errors: ' + str(errors))
    logger.info(str(t))
