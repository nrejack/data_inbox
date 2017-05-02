#!/usr/bin/env python

"""
data_inbox: data quality checker
"""

import logging
import sqlite3
import shutil
import click
import fileset_db
import os

PARTNER_DATA_FILE = 'partner_data.sql'
PARTNER_ERROR_CODES_DATA_FILE = 'partner_error_codes.sql'
FILE_ERROR_CODES_DATA_FILE = 'file_error_codes.sql'
FILETYPES_DATA_FILE = 'filetypes.sql'
FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'

@click.option('-v', '--verbose', help='Run in verbose mode.', is_flag=True, default=False)
@click.option('-n', '--nocreate', help='Don\'t prompt for creation of tables and loading data.', is_flag=True, default=False)
@click.command()
def main(verbose, nocreate):
    logger = configure_logging(verbose)

    logger.info("Starting data_inbox.py")
    # open database
    logger.info("Backing up database {} to {}".format(FILESET_DATABASE, FILESET_DATABASE_BACKUP))
    #shutil.copy2(FILESET_DATABASE, FILESET_DATABASE_BACKUP)
    logger.info("Opening database: {}".format(FILESET_DATABASE))
    conn = sqlite3.connect(FILESET_DATABASE)
    conn.row_factory = dict_factory
    c = conn.cursor()

    # set up tables
    if not nocreate:
        create_sql = input("Do you wish to create the needed SQL tables? (y/n)")
        if(create_sql.upper() == 'Y'):
            logger.info("Creating necessary tables.")
            try:
                fileset_db.create_empty_tables(conn)
            except sqlite3.OperationalError:
                logger.error("Error: table already exists.")
            logger.info("Committing changes to {}".format(FILESET_DATABASE))
            conn.commit()
        else:
            logger.info("Skipping table creation.")

    # read data into table
    if not nocreate:
        for sql_file in [PARTNER_DATA_FILE, PARTNER_ERROR_CODES_DATA_FILE, FILE_ERROR_CODES_DATA_FILE, FILETYPES_DATA_FILE]:
            read_in_sql_files(sql_file, logger, conn)

    # read list of partners from table
    partner_list_query = "SELECT name, name_full, file_directory FROM partners"
    partner_info = []
    logger.debug("List of partners")
    for row in conn.execute(partner_list_query):
            logger.debug(row)
            partner_info.append(row)

    # check partner dirs for changes in headers
    check_partner_dirs(partner_info, conn, logger)

    # report on results
    run_report(conn, logger)

    logger.info("Committing changes to {}".format(FILESET_DATABASE))
    conn.commit()
    logger.info("Closing {}".format(FILESET_DATABASE))
    conn.close()

def read_in_sql_files(sql_file, logger, conn):
    create_sql = input("Do you wish to read the {} file into the tables? (y/n)".format(sql_file))
    if(create_sql.upper() == 'Y'):
        logger.info("Reading in data.")
        with open(sql_file, 'r') as myfile:
            sql_file_data = myfile.read()
        logger.info("Read in {} bytes from {}".format(len(sql_file_data), sql_file))
        logger.info(sql_file_data)
        conn.execute(sql_file_data)
        logger.info("Loaded {} into database.".format(sql_file))
    else:
        logger.info("Skipping reading in data.")
    logger.info("Committing changes to {}".format(FILESET_DATABASE))
    conn.commit()

def run_report(conn, logger):
    """Report status of current run."""
    logger.debug("Current run status")
    report = conn.execute("SELECT * FROM run_status")
    for row in report:
        logger.debug(row)

def check_partner_dirs(partner_info, conn, logger):
    """Iterate over partners and check each for issues.
        get list of partners from TABLE
        iterate over list
            check if directory is empty
                yes: insert row saying error code 1
                no: header inspection

    """

    # get list of partners and locations of files
    partner_list_query =("SELECT name, name_full, file_directory FROM partners")
    partner_list = conn.execute(partner_list_query)
    for partner in partner_list:
        logger.info("Now checking {}".format(partner['name_full']))
        if not os.path.isdir(partner['file_directory']):
            logger.error("Path {} not found for {}".format(partner['file_directory'], partner['name_full']))
            error_code = 2
        elif not os.listdir(partner['file_directory']):
            logger.info("Path {} empty found for {}".format(partner['file_directory'], partner['name_full']))
            error_code = 1

def dict_factory(cursor, row):
    """Helper function to return dictionary."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def configure_logging(verbose):
    """Configure the logger."""
    # set up logging
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG)
    # logging to file
    fh = logging.FileHandler('data_inbox.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # logging to console
    ch = logging.StreamHandler()
    if verbose:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.WARNING)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

if __name__ == '__main__':
    main()
