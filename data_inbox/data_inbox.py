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
import datetime

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
        else:
            logger.info("Skipping table creation.")
        commit_tran(conn, logger)


    # read data into table
    if not nocreate:
        for sql_file in [PARTNER_DATA_FILE, PARTNER_ERROR_CODES_DATA_FILE, FILE_ERROR_CODES_DATA_FILE, FILETYPES_DATA_FILE]:
            read_in_sql_files(sql_file, logger, conn)

    # read list of partners from table
    partner_list_query = "SELECT id, name, name_full, file_directory FROM partners"
    partner_info = {}
    logger.debug("Dictionary of partners")
    for row in conn.execute(partner_list_query):
            logger.debug(row)
            partner_info[row['id']] = row

    # get the next key for the global_run_status
    global_run_status_id = check_run_id(c, conn, logger)

    # check partner dirs for changes in headers
    check_partner_dirs(partner_info, conn, logger, global_run_status_id)

    # report on results
    run_report(conn, logger, global_run_status_id, partner_info)
    commit_tran(conn, logger)
    logger.info("Closing {}".format(FILESET_DATABASE))
    conn.close()

def check_run_id(c, conn, logger):
    """Check the previous run's ID and get the current run's ID.
        Update the DB with the new record."""
    logger.info("Getting the next ID for the global_run_status")
    c.execute("SELECT MAX(id) FROM global_run_status;")
    penultiumate_run_id = c.fetchone()['MAX(id)']
    logger.debug("Penultiumate run ID: {}".format(penultiumate_run_id))
    if penultiumate_run_id is None:
        global_run_status_id = 1
    else:
        global_run_status_id = penultiumate_run_id + 1
    logger.info("This is global_run_status id {}".format(global_run_status_id))
    # update the run table
    current_date = datetime.datetime.now()
    conn.execute("INSERT INTO global_run_status (run_date) VALUES (current_date)")
    return global_run_status_id

def commit_tran(conn, logger):
    """Helper function to commit changes to DB."""
    logger.info("Committing changes to {}".format(FILESET_DATABASE))
    conn.commit()

def read_in_sql_files(sql_file, logger, conn):
    """Read passed in .sql files to load their data into the DB."""
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
    commit_tran(conn, logger)

def run_report(conn, logger, global_run_status_id, partner_info):
    """Report status of current run."""
    logger.debug("Current run status")
    #logger.debug(partner_info)
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", (int(global_run_status_id),))
    # TODO refactor this for reporting vs. checking what the status is
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['file_directory']
        if row['code'] == 1:
            logger.info("Partner {} has no new data in the current run {}".format(partner_name, global_run_status_id))
        if row['code'] == 2:
            logger.info("Partner {} directory {} not found in the current run {}".format(partner_name, partner_directory, global_run_status_id))
        if row['code'] == 3:
            logger.info("Partner {} has new files in the current run {}".format(partner_name, global_run_status_id))

def check_partner_dirs(partner_info, conn, logger, global_run_status_id):
    """Iterate over partners and check to see if there are files."""
    # get list of partners and locations of files
    partner_list_query =("SELECT id, name, name_full, file_directory FROM partners")
    partner_list = conn.execute(partner_list_query)
    for partner in partner_list:
        error_code = None
        logger.info("Now checking {}".format(partner['name_full']))
        if not os.path.isdir(partner['file_directory']):
            logger.error("Path {} not found for {}".format(partner['file_directory'], partner['name_full']))
            error_code = 2  # directory missing
        elif not os.listdir(partner['file_directory']):
            logger.info("Path {} empty found for {}".format(partner['file_directory'], partner['name_full']))
            error_code = 1  # no files, therefore no new files
        else:
            error_code = 3
        if error_code:
            conn.execute('INSERT INTO partner_run_status (code, partner, run_id) VALUES (?, ?, ?)', (error_code, partner['id'], global_run_status_id))

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
