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
FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'

@click.option('-v', '--verbose', help='Run in verbose mode.', is_flag=True, default=False)
@click.command()
def main(verbose):
    logger = configure_logging(verbose)

    logger.info("Starting data_inbox.py")
    # open database
    logger.info("Backing up database {} to {}".format(FILESET_DATABASE, FILESET_DATABASE_BACKUP))
    #shutil.copy2(FILESET_DATABASE, FILESET_DATABASE_BACKUP)
    logger.info("Opening database: {}".format(FILESET_DATABASE))
    conn = sqlite3.connect(FILESET_DATABASE)
    c = conn.cursor()

    # set up tables
    create_sql = input("Do you wish to create the needed SQL tables? (y/n)")
    if(create_sql.upper() == 'Y'):
        logger.info("Creating necessary tables.")
        fileset_db.create_empty_tables(conn)
        logger.info("Committing changes to {}".format(FILESET_DATABASE))
        conn.commit()
        logger.info("Closing {}".format(FILESET_DATABASE))
    else:
        logger.info("Skipping table creation.")

    # read data into table
    create_sql = input("Do you wish to read the data into the tables? (y/n)")
    if(create_sql.upper() == 'Y'):
        logger.info("Reading in data.")
        with open(PARTNER_DATA_FILE, 'r') as myfile:
            partner_data = myfile.read()
        logger.info("Read in {} bytes from {}".format(len(partner_data), PARTNER_DATA_FILE))
        conn.execute(partner_data)
        logger.info("Loaded {} into database.".format(PARTNER_DATA_FILE))
    else:
        logger.info("Skipping reading in data.")


    # read list of partners from table
    partner_list_query = "SELECT * FROM partners"
    partner_list = conn.execute(partner_list_query)
    for item in partner_list:
        logger.debug(item)
    conn.close()


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
