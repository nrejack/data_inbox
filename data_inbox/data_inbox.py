#!/usr/bin/env python

"""
data_inbox: data quality checker
"""

import logging
import sqlite3
import shutil
import os
import datetime
import click
import fileset_db

PARTNER_DATA_FILE = 'partner_data.sql'
PARTNER_ERROR_CODES_DATA_FILE = 'partner_error_codes.sql'
FILE_ERROR_CODES_DATA_FILE = 'file_error_codes.sql'
FILETYPES_DATA_FILE = 'filetypes.sql'
FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'

@click.option('-v', '--verbose', help='Run in verbose mode.', \
    is_flag=True, default=False)
@click.option('-n', '--nocreate', help='Don\'t prompt for creation of tables \
    and loading data.', is_flag=True, default=False)
@click.command()
def main(verbose, nocreate):
    """main function for data_inbox."""
    logger = configure_logging(verbose)

    logger.info("Starting data_inbox.py")
    # backup db if it already exists
    if os.path.isfile(FILESET_DATABASE):
        logger.info("Backing up database {} to {}".format(FILESET_DATABASE, \
            FILESET_DATABASE_BACKUP))
        shutil.copy2(FILESET_DATABASE, FILESET_DATABASE_BACKUP)
    # open existing db
    logger.info("Opening database: {}".format(FILESET_DATABASE))
    conn = sqlite3.connect(FILESET_DATABASE)
    conn.row_factory = dict_factory
    c = conn.cursor()

    # set up tables
    if not nocreate:
        create_sql = input("Do you wish to create the needed SQL tables? (y/n)")
        if create_sql.upper() == 'Y':
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
        for sql_file in [PARTNER_DATA_FILE, PARTNER_ERROR_CODES_DATA_FILE, \
                        FILE_ERROR_CODES_DATA_FILE, FILETYPES_DATA_FILE]:
            read_in_sql_files(sql_file, logger, conn)

    # read list of partners from table
    # TODO: abstract the data access methods
    partner_list_query = "SELECT id, name, name_full, file_directory FROM partners"
    partner_info = {}
    logger.debug("Dictionary of partners")
    for row in conn.execute(partner_list_query):
        logger.debug(row)
        partner_info[row['id']] = row

    # get the next key for the run id
    current_run_id = check_run_id(c, conn, logger)

    # check partner dirs for files
    check_partner_dirs(partner_info, conn, logger, current_run_id)

    # report on results
    run_report(conn, logger, current_run_id, partner_info)

    # check partner files for headers
    # TODO move this back to before run_report
    check_partner_files(partner_info, conn, logger, current_run_id)

    commit_tran(conn, logger)
    logger.info("Closing {}".format(FILESET_DATABASE))
    conn.close()

def check_run_id(c, conn, logger):
    """Check the previous run's ID and get the current run's ID.
        Update the DB with the new record."""
    logger.info("Getting the next ID for the current run")
    c.execute("SELECT MAX(id) FROM current_run_status;")
    penultiumate_run_id = c.fetchone()['MAX(id)']
    if penultiumate_run_id is None:
        current_run_id = 1
        penultiumate_run_id = 0
    else:
        current_run_id = penultiumate_run_id + 1
    logger.debug("Penultiumate run ID: %i", penultiumate_run_id)
    logger.info("This is current_run_status id {}".format(current_run_id))
    # update the run table
    current_date = datetime.datetime.now()
    conn.execute("INSERT INTO current_run_status (run_date) VALUES (current_date)")
    return current_run_id

def commit_tran(conn, logger):
    """Helper function to commit changes to DB."""
    logger.info("Committing changes to {}".format(FILESET_DATABASE))
    conn.commit()

def read_in_sql_files(sql_file, logger, conn):
    """Read passed in .sql files to load their data into the DB."""
    create_sql = input("Do you wish to read the {} file into the tables? (y/n)".format(sql_file))
    if create_sql.upper() == 'Y':
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

def run_report(conn, logger, current_run_id, partner_info):
    """Report status of current run."""
    logger.debug("Current run status")
    #logger.debug(partner_info)
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", \
        (int(current_run_id),))
    # TODO refactor this for reporting vs. checking what the status is
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['file_directory']
        if row['code'] == 1:
            logger.info("Partner {} has no new data in the current run {}"\
                        .format(partner_name, current_run_id))
        if row['code'] == 2:
            logger.info("Partner {} directory {} not found in the current run {}\
                ".format(partner_name, partner_directory, current_run_id))
        if row['code'] == 3:
            logger.info("Partner {} has new files in the current run {}" \
                .format(partner_name, current_run_id))

def check_partner_dirs(partner_info, conn, logger, current_run_id):
    """Iterate over partners and check to see if there are files."""
    # get list of partners and locations of files
    partner_list_query = ("SELECT id, name, name_full, file_directory FROM partners")
    partner_list = conn.execute(partner_list_query)
    for partner in partner_list:
        error_code = None
        logger.info("Now checking {}".format(partner['name_full']))
        if not os.path.isdir(partner['file_directory']):
            logger.error("Path {} not found for {}" \
                .format(partner['file_directory'], partner['name_full']))
            error_code = 2  # directory missing
        elif not os.listdir(partner['file_directory']):
            logger.info("Path {} empty found for {}" \
                .format(partner['file_directory'], partner['name_full']))
            error_code = 1  # no files, therefore no new files
        else:
            error_code = 3
        if error_code:
            conn.execute('INSERT INTO partner_run_status \
                (code, partner, run_id) VALUES (?, ?, ?)', \
                (error_code, partner['id'], current_run_id))

def check_partner_files(partner_info, conn, logger, current_run_id):
    # TODO refactor this to avoid the duplication in the reporting function
    """Iterate over files for each partner to check their headers."""
    logger.debug("Checking partner files")
    partners_to_check = []
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", (int(current_run_id),))
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['file_directory']
        partner_id = partner_info[row['partner']]['id']
        if row['code'] == 1:
            pass # no new data
        if row['code'] == 2:
            pass # directory not found
        if row['code'] == 3:
            logger.info("Adding partner {} to the list to check".format(partner_name))
            partners_to_check.append({'id':partner_id, 'name_full':partner_name})

    # TODO make new function here
    # now check the list
    for partner in partners_to_check:
        name_full = partner['name_full']
        pid = partner['id']
        logger.info("Now checking {} ".format(name_full))
        # load the previous fileset info
        partner_fileset = load_previous_fileset(conn, pid, logger, name_full)
        new_fileset = os.listdir(partner_directory)
        for new_file in new_fileset:
            logger.debug("Now checking file {}".format(new_file))
            # find a match
            # search the fileset to find a matching filename
            for row in partner_fileset:
                # previous filename
                filename = row['filename_pattern'].split('.')[0]
                # current filename check
                new_file_trim = new_file.split('.')[0]
                if new_file_trim == filename:
                    logger.info("Match found: {}".format(filename))
                    check_header(new_file, partner_directory, row['header'], logger)
                else:
                    logger.info("match not found for {} {}".format(new_file_trim, filename))

def load_previous_fileset(conn, pid, logger, name_full):
    """Load previous fileset for a specified (pid) partner."""
    partner_fileset_sql = conn.execute("SELECT * FROM \
        partners_filesets WHERE pid=?", (int(pid),))

    #for item in partner_fileset_sql:
    #    logger.debug(item)

    partner_fileset = []
    for item in partner_fileset_sql:
        partner_fileset.append({'pid': item['pid'], \
            'filetype': item['filetype'], \
            'filename_pattern': item['filename_pattern'], \
            'header': item['header']})

    logger.info("Loaded previous fileset for {}".format(name_full))
    if len(partner_fileset) == 0:
        logger.warning("No previous recorded fileset stored for {}".format(name_full))
        logger.warning("Adding new files to database.")
        logger.fatal("NOT IMPLEMENTED: method to add new files to db")
        logger.fatal('{} will not be checked in this run.'.format(name_full))
    else:
        logger.debug(partner_fileset)
    return partner_fileset

def check_header(new_file, partner_directory, prev_header, logger):
    """Check new file header against previous header."""
    # TODO: make this work on non-exact matching
    with open(partner_directory + new_file, 'r') as f:
        header_row = f.readline().strip()
        logger.debug("Header for file {}:\n{}".format(new_file, header_row.strip()))
        header_cols = header_row.split(",")
        prev_header = prev_header.split(",")
        logger.debug(header_cols)
        logger.debug(prev_header)
        # iterate over old header, deleting any cols in new header that match
        for old_col in prev_header:
            logger.debug("Column header match: {}".format(old_col))
            header_cols.remove(old_col)

        logger.debug("After matching columns: {}".format(len(header_cols)))
        if len(header_cols) == 0:
            logger.info("{} header matches old header. No change in header.".format(new_file))

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
