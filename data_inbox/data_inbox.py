#!/usr/bin/env python

"""
data_inbox: data quality checker
"""

import logging
import sqlite3
import shutil
import click
import fileset_db

PARTNER_DATA_FILE = 'partner_data.sql'
ERROR_CODES_DATA_FILE = 'error_codes.sql'
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
            fileset_db.create_empty_tables(conn)
            logger.info("Committing changes to {}".format(FILESET_DATABASE))
            conn.commit()
        else:
            logger.info("Skipping table creation.")

    # read data into table
    if not nocreate:
        for sql_file in [PARTNER_DATA_FILE, ERROR_CODES_DATA_FILE]:
            read_in_sql_files(sql_file, logger, conn)

    # read list of partners from table
    partner_list_query = "SELECT * FROM partners"
    partner_info = []
    logger.debug("List of partners")
    for row in conn.execute(partner_list_query):
            logger.debug(row)
            partner_info.append(row)

    # check partner dirs for changes in headers
    check_partner_dirs(partner_info)

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



def check_partner_dirs(partner_info):
    """Iterate over partners and check each for issues."""

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
