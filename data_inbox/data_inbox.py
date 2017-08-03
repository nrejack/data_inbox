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
import time
from fuzzywuzzy import fuzz
import smtplib
from email.mime.text import MIMEText

PARTNER_DATA_FILE = 'partner_data.sql'
PARTNER_ERROR_CODES_DATA_FILE = 'partner_error_codes.sql'
FILE_ERROR_CODES_DATA_FILE = 'file_error_codes.sql'
FILETYPES_DATA_FILE = 'filetypes.sql'
FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'
# set the minimum match ratio for fuzzy matching
MATCH_RATIO = 80

@click.option('-v', '--verbose', help='Run in verbose mode.', \
    is_flag=True, default=False)
@click.option('-n', '--nocreate', help='Don\'t prompt for creation of tables \
    and loading data.', is_flag=True, default=False)
@click.option('-b', '--buildfileset', help='Build fileset for partner.', \
    is_flag=True, default=False)
@click.command()
def main(verbose, nocreate, buildfileset):
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
        setup_tables(conn, logger)

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

    # build fileset, and stop execution after
    if buildfileset:
        add_new_fileset(conn, logger)
        exit()
    # check partner dirs for files
    check_partner_dirs(partner_info, conn, logger, current_run_id)

    # report on partner results
    report = run_partner_report(conn, logger, current_run_id, partner_info)

    # check partner files for headers
    check_partner_files(partner_info, conn, logger, current_run_id)

    # run file report
    report += run_file_report(conn, logger, current_run_id, partner_info)

    print("*****************")
    print(report)
    print("*****************")

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

def run_partner_report(conn, logger, current_run_id, partner_info):
    """Report status of current run."""
    output_report = ""
    logger.debug("Current run status")
    output_report += "OneFlorida Data Trust partner file check for " + str(datetime.datetime.now()) + "\n\n\nPartner-level summaries\n-----------------------\n\n"
    no_new_data = "No new data\n--------------------\n"
    dir_not_found = "Directory not found\n--------------------\n"
    new_files = "New files\n--------------------\n"

    #logger.debug(partner_info)
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", \
        (int(current_run_id),))
    # TODO refactor this for reporting vs. checking what the status is
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['file_directory']
        if row['code'] == 1:
            message = "Partner {} has no new data in the current run {}\n"\
                        .format(partner_name, current_run_id)
            logger.info(message)
            no_new_data += partner_name + "\n"

        if row['code'] == 2:
            message = "Partner {} directory {} not found in the current run {}\n\
                ".format(partner_name, partner_directory, current_run_id)
            logger.info(message)
            dir_not_found += partner_name + "\n"

        if row['code'] == 3:
            message = "Partner {} has new files in the current run {}\n" \
                .format(partner_name, current_run_id)
            logger.info(message)
            new_files += partner_name + "\n"

    output_report += no_new_data + "\n" + dir_not_found + "\n" + new_files + "\n"
    return output_report

def run_file_report(conn, logger, current_run_id, partner_info):
    report = "\n\n\nDetailed report\n--------------------\n\n"
    # first get list of partners we need to report on
    partner_list = conn.execute("SELECT partner FROM partner_run_status WHERE code = 3 AND run_id = ?", (str(current_run_id),))
    # next, get the file error codes
    for partner in partner_list:
        # get the partner name\
        partner_code = partner['partner']
        partner_name = conn.execute("SELECT name_full from partners where id = ?", (partner['partner'],)).fetchone()
        file_list = conn.execute("SELECT code, partner, run_id, filename_pattern FROM file_run_status WHERE run_id = ? AND partner = ?", (int(current_run_id), partner_code))
        do_once = True
        for item in file_list:
            while do_once:
                report += partner_name['name_full'] +"\n"
                report += "-----------------------\n"
                do_once = False
            logger.debug("Partner {} {} ".format(partner_code, partner_name))
            logger.debug(item)
            if item['code'] == 1:
                report += "{} has no change in header. OK to process.\n".format(item['filename_pattern'])
            elif item['code'] == 2:
                # TODO: store new column names and print them here
                report += "{} has a new column. Check before processing.\n".format(item['filename_pattern'])
            elif item['code'] == 3:
                # TODO: store deleted column name and print it here
                report += "{} is missing a previously existing column. Check before processing.\n".format(item['filename_pattern'])
            elif item['code'] == 4:
                report += "{} may be missing a header. No previous column names matched. Check before processing.\n".format(item['filename_pattern'])
            elif item['code'] == 5:
                report += "{} is a new or unidentified filetype. Update fileset_db to match.\n".format(item['filename_pattern'])
        do_once = True
        while do_once:
            report += "\n"
            do_once = False
    return report

def setup_tables(conn, logger):
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
                # default is no match
                status = 5
                # ignore case in comparison
                if new_file_trim.upper() == filename.upper():
                    logger.info("Match found: {}".format(filename))
                    status = check_header(new_file, partner_directory, row['header'], logger)[0]
                if status == 1:
                    result = conn.execute("INSERT INTO file_run_status (code, partner, run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", (1, pid, current_run_id, filename, "unknown"))
                elif status == 2:
                    result = conn.execute("INSERT INTO file_run_status (code, partner, run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", (2, pid, current_run_id, filename, "unknown"))
                elif status == 3:
                    result = conn.execute("INSERT INTO file_run_status (code, partner, run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", (3, pid, current_run_id, filename, "unknown"))
                elif status == 4:
                    result = conn.execute("INSERT INTO file_run_status (code, partner, run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", (4, pid, current_run_id, filename, "unknown"))
                elif status == 5:
                    logger.info("match not found for {} {}".format(new_file_trim, filename))
                    result = conn.execute("INSERT INTO file_run_status (code, partner, run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", (5, pid, current_run_id, filename, "unknown"))

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

    logger.info("Loading previous fileset for {}".format(name_full))
    if len(partner_fileset) == 0:
        logger.warning("No previous recorded fileset stored for {}".format(name_full))
        logger.fatal('{} will not be checked in this run.'.format(name_full))
    else:
        logger.debug(partner_fileset)
    return partner_fileset

def add_new_fileset(conn, logger):
    """Read files on disk and build a new fileset record for the partner."""
    partner_list = get_partner_list(conn, logger)
    filetype_dict = get_filetype_dict(conn, logger)
    logger.info("Current list of partners")
    logger.info(partner_list)
    for partner in partner_list:
        name = partner_list[partner]['name']
        directory = partner_list[partner]['file_directory']
        try:
            list_of_files = os.listdir(directory)
        except:
            logger.error("Directory {} not found.".format(directory))
            continue
        if not list_of_files:
            logger.info("File directory empty for {}. Skipping building fileset.".format(name))
        else:
            logger.info("Now building fileset for {}".format(name))
            for new_file in list_of_files:
                with open(directory + new_file, 'r') as f:
                    header = f.readline()
                logger.info("Now trying to add file {} to fileset.".format(new_file))
                guess_filetype(partner, new_file, filetype_dict, header, conn, logger)

def get_filetype_dict(conn, logger):
    """Utility function that gets the dict of filetypes."""
    # TODO replace * in SELECT with column names
    filetypes = conn.execute("SELECT * FROM filetypes")
    filetypes_dict = {}
    for item in filetypes:
        filetypes_dict[item['filetype_name'].upper()] = item['filetype_id']
    return filetypes_dict

def get_partner_list(conn, logger):
    """Utility function that gets the list of partners and their attributes"""
    partner_data = conn.execute("SELECT * FROM partners")
    partner_dict = {}
    for partner in partner_data:
        partner_key = partner['id']
        partner_dict[partner_key] = {'name': partner['name'], 'file_directory': partner['file_directory']}
    return partner_dict

def guess_filetype(partner, new_file, filetype_dict, header, conn, logger):
    """Given an incoming file, guess the filetype."""
    # TODO restructure this to streamline the matching logic
    #logger.info(filetype_dict)
    # take the file extension off
    new_file = new_file.split('.')[0]
    new_file_upper = new_file.upper()
    file_matched = False
    # try exact match (case doesn't matter) first
    if new_file_upper in filetype_dict.keys():
        logger.info("Exact match found: {} looks like a {} file.".format(new_file, new_file.upper()))
        filetype_id = filetype_dict[new_file_upper]
        add_to_filetype_dict(partner, filetype_id, new_file, header, conn, logger)
        file_matched = True
    # try to find as a substring or do fuzzy match
    if not file_matched:
        for item in filetype_dict.keys():
            filetype_id = filetype_dict[item]
            #logger.debug(item)
            #logger.debug(new_file_upper)
            #logger.debug(fuzz.ratio(new_file_upper, item))
            if fuzz.ratio(new_file_upper, item) > MATCH_RATIO or new_file_upper.find(item) != -1:
                logger.info("Partial match found for {}: looks like a {} file".format(new_file, item))
                add_to_filetype_dict(partner, filetype_id, new_file, header, conn, logger)
                file_matched = True
                break
    if not file_matched:
        logger.info("No full match found for {}. Not sure what it is.".format(new_file))

def add_to_filetype_dict(partner, filetype_id, new_file, header, conn, logger):
    """Function to add a new file to the filetype dictionary."""
    logger.info("Adding new filetype record for partner {} for filetype_id {}".format(partner, filetype_id))
    date_now = time.strftime('%Y-%m-%d %H:%M:%S')
    # get existing filetype record
    existing_filetype_row = conn.execute("SELECT * FROM partners_filesets WHERE pid = ? AND filetype = ?", (partner, filetype_id))
    if existing_filetype_row:
        logger.info("Existing filetype record found for partner {} and filetype {}".format(partner, filetype_id))
        logger.info("Deleting previous filetype records from partners_filesets")
        delete_tran = conn.execute("DELETE FROM partners_filesets WHERE pid = ? AND filetype = ?", (partner, filetype_id))
        commit_tran(conn, logger)
    else:
        logger.info("No existing filetype record found for partner {} and filetype {}".format(partner, filetype_id))
    logger.info("Adding new filetype record.")
    result = conn.execute("INSERT INTO partners_filesets (pid, date, filename_pattern, filetype, header) VALUES (?, ?, ?, ?, ?)", (int(partner), date_now, new_file, filetype_id, header))
    commit_tran(conn, logger)
    #logger.warning("add_to_filetype_dict not yet implemented. NOOP")

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
        starting_new_header_len = len(header_cols)
        # iterate over old header, deleting any cols in new header that match
        for old_col in prev_header:
            logger.debug("Column header match: {}".format(old_col))
            try:
                header_cols.remove(old_col)
            except ValueError:
                logger.info("Old column {} has been deleted.".format(old_col))
                return [3]
        logger.debug("After matching columns: {}".format(len(header_cols)))
        if len(header_cols) == 0:
            logger.info("{} header matches old header. No change in header.".format(new_file))
            return [1]
        elif len(header_cols) == starting_new_header_len:
            # if the length hasn't changed, assume header is missing in file
            logger.info("No columns from new header match old header.".format(new_file))
            logger.info("Header may haven been deleted.")
            return [4]
        else:
            logger.info("{} header does not match old header.".format(new_file))
            logger.info("New column(s) found: {}".format(header_cols))
            return [2, header_cols]

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


def send_report(report, from_address, to_address, mail_server):
    """Sends the report via email.
    Keyword arguments:
    report -- the report as a string.
    from_address -- email address the report will come from.
    to_address -- email address the report is going to.
    """
    msg = MIMEText(report)
    msg['Subject'] = \
        "data_inbox report for " + str(datetime.now()) + "\n"
    msg['From'] = from_address
    msg['To'] = to_address
    mail_connection = smtplib.SMTP(mail_server)
    mail_connection.sendmail(from_address, to_address, msg.as_string())
    mail_connection.quit()

if __name__ == '__main__':
    main()
