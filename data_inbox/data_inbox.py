#!/usr/bin/env python

"""
data_inbox: data quality checker
"""

# TODO!!: Add database abstraction layer

import time
import smtplib
from email.mime.text import MIMEText
from string import digits
import logging
import sqlite3
import shutil
import os
from datetime import date
from subprocess import call

import datetime
import click
import fileset_db
from fuzzywuzzy import fuzz

VERSION_NUMBER = '0.2'
PARTNER_DATA_FILE = 'partner_data.sql'
PARTNER_ERROR_CODES_DATA_FILE = 'partner_error_codes.sql'
FILE_ERROR_CODES_DATA_FILE = 'file_error_codes.sql'
FILETYPES_DATA_FILE = 'filetypes.sql'
FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'
FILETYPES_TO_SKIP = ['pdf', 'xlsx', 'xls', 'zip']
# set the minimum match ratio for fuzzy matching
MATCH_RATIO = 80

FROM_EMAILS = 'please-do-not-reply@ufl.edu'
TO_EMAILS = 'nrejack@ufl.edu'
MAIL_SERVER = 'smtp.ufl.edu'

@click.option('-v', '--verbose', help='Run in verbose mode.', \
    is_flag=True, default=False)
@click.option('-c', '--create', help='Prompt for creation of tables \
    and loading data.', is_flag=True, default=False)
@click.option('-b', '--buildfileset', help='Build fileset for partner.', \
    is_flag=True, default=False)
#@click.option('-m', '--manual', help='Run in manual mode.', is_flag=True, \
#cd da    default=False)
@click.command()
def main(verbose, create, buildfileset):
    """main function for data_inbox."""
    logger = configure_logging(verbose)

    logger.info("Starting data_inbox.py")
    # backup db if it already exists
    if os.path.isfile(FILESET_DATABASE):
        logger.info("Backing up database %s to %s", FILESET_DATABASE, \
            FILESET_DATABASE_BACKUP)
        shutil.copy2(FILESET_DATABASE, FILESET_DATABASE_BACKUP)
    # open existing db
    logger.info("Opening database: %s", FILESET_DATABASE)
    conn = sqlite3.connect(FILESET_DATABASE)
    conn.row_factory = dict_factory
    conn_cursor = conn.cursor()

    # set up tables
    if create:
        setup_tables(conn, logger)

    # read data into table
    if create:
        for sql_file in [PARTNER_DATA_FILE, PARTNER_ERROR_CODES_DATA_FILE, \
                        FILE_ERROR_CODES_DATA_FILE, FILETYPES_DATA_FILE]:
            read_in_sql_files(os.path.join('sql', sql_file), logger, conn)
    if create:
        exit()

    # read list of partners from table
    # TODO: abstract the data access methods
    partner_list_query = "SELECT id, name, name_full, incoming_file_directory FROM partners"
    partner_info = {}
    logger.debug("Dictionary of partners")
    for row in conn.execute(partner_list_query):
        logger.debug(row)
        partner_info[row['id']] = row

    # get the next key for the run id
    current_run_id = check_run_id(conn_cursor, conn, logger)

    # build fileset, and stop execution after
    if buildfileset:
        add_new_fileset(conn, logger)
        exit()
    # check partner dirs for files
    check_partner_dirs(partner_info, conn, logger, current_run_id)

    # report on partner results
    temp_report = run_partner_report(conn, logger, current_run_id, partner_info)

    # check partner files for headers
    check_partner_files(partner_info, conn, logger, current_run_id)

    # generate exception report
    report = generate_exception_report(conn, logger, current_run_id, partner_info)

    # add the partner results after the exception
    report += temp_report

    # run file report
    report += run_file_report(conn, logger, current_run_id, partner_info)

    #send_report(report, FROM_EMAILS, TO_EMAILS, MAIL_SERVER)
    write_report(report, logger)
    print("*****************")
    print(report)
    print("*****************")

    commit_tran(conn, logger)
    logger.info("Closing %s", FILESET_DATABASE)
    conn.close()

def check_run_id(conn_cursor, conn, logger):
    """Check the previous run's ID and get the current run's ID.
        Update the DB with the new record."""
    logger.info("Getting the next ID for the current run")
    conn_cursor.execute("SELECT MAX(id) FROM current_run_status;")
    penultiumate_run_id = conn_cursor.fetchone()['MAX(id)']
    if penultiumate_run_id is None:
        current_run_id = 1
        penultiumate_run_id = 0
    else:
        current_run_id = penultiumate_run_id + 1
    logger.debug("Penultiumate run ID: %i", penultiumate_run_id)
    logger.info("This is current_run_status id %i", current_run_id)
    # update the run table
    current_date = datetime.datetime.now()
    conn.execute("INSERT INTO current_run_status (run_date) VALUES (current_date)")
    return current_run_id

def commit_tran(conn, logger):
    """Helper function to commit changes to DB."""
    logger.info("Committing changes to %s", FILESET_DATABASE)
    conn.commit()

def read_in_sql_files(sql_file, logger, conn):
    """Read passed in .sql files to load their data into the DB."""
    create_sql = input("Do you wish to read the {}file into the tables?(y/n) " \
        .format(sql_file))
    if create_sql.upper() == 'Y':
        logger.info("Reading in data.")
        with open(sql_file, 'r') as myfile:
            sql_file_data = myfile.read()
        logger.info("Read in %i bytes from %s", len(sql_file_data), sql_file)
        logger.info(sql_file_data)
        conn.execute(sql_file_data)
        logger.info("Loaded %s into database.", sql_file)
    else:
        logger.info("Skipping reading in data.")
    commit_tran(conn, logger)

def run_partner_report(conn, logger, current_run_id, partner_info):
    """Report status of current run."""
    output_report = ""
    logger.debug("Current run status")
    output_report = "\n\n\nPartner-level summaries\n-----------------------\n\n"
    no_new_data = "No new data\n--------------------\n"
    no_new_data_initial_len = len(no_new_data)
    dir_not_found = "Directory not found\n--------------------\n"
    dir_not_found_initial_len = len(dir_not_found)
    new_files = "New data\n--------------------\n"
    new_files_initial_len = len(new_files)
    not_checked = "Not checked\n--------------------\n"
    not_checked_initial_len = len(not_checked)
    #logger.debug("Checking for violations.")
    #generate_violation_report(partner_info, conn, logger, current_run_id)

    #logger.debug(partner_info)
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", \
        (int(current_run_id),))
    # TODO refactor this for reporting vs. checking what the status is
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['incoming_file_directory']
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

        if row['code'] == 4:
            message = "Partner {} is set to not be checked {}\n" \
                .format(partner_name, current_run_id)
            logger.info(message)
            not_checked += partner_name + "\n"

    if len(no_new_data) > no_new_data_initial_len:
        output_report += no_new_data + "\n"

    if len(dir_not_found) > dir_not_found_initial_len:
        output_report += dir_not_found + "\n"

    if len(new_files) > new_files_initial_len:
        output_report += new_files + "\n"

    if len(not_checked) > not_checked_initial_len:
        output_report += not_checked + "\n"

    return output_report

def generate_exception_report(conn, logger, current_run_id, partner_info):
    """Generate exception report."""
    logger.info("Starting exception report.")
    report = "OneFlorida Data Trust partner file check for " \
            + str(datetime.datetime.now())
    report += "\n\n\n :: Exceptions ::\n--------------------\n"
    # # get list of partners to check
    # partner_list = []
    # partners_to_check = conn.execute("SELECT partner FROM partner_run_status WHERE code = 3 AND run_id=?", \
    #     (str(current_run_id),))
    # for item in partners_to_check:
    #     partner_list.append(item['partner'])
    # if len(partner_list) == 0:
    #     report += "None noted.\n"
    #     return report
    # else:
    #     for partner in partner_list:
    #         file_list = conn.execute("SELECT code, partner, run_id, \
    #             filename_pattern, cols_add, cols_del FROM file_run_status WHERE \
    #             run_id = ? AND partner = ?", (int(current_run_id), partner))
    #         for current_file in file_problems:
    #             if current_file['code'] != 1:
    #                 print("problem found: %s", get_file_status)
    #     return report
    report += run_file_report(conn, logger, current_run_id, partner_info, detailed = False)
    return report

def run_file_report(conn, logger, current_run_id, partner_info, detailed = True):
    report = ""
    if detailed == True:
        report = "\n\n\nDetailed report\n--------------------\n\n"
    # first get list of partners we need to report on
    partner_list = conn.execute("SELECT partner FROM partner_run_status \
        WHERE code = 3 AND run_id = ?", (str(current_run_id),))
    # next, get the file error codes
    for partner in partner_list:
        # get the partner name\
        partner_code = partner['partner']
        partner_name = conn.execute("SELECT name_full from partners \
            WHERE id = ?", (partner['partner'],)).fetchone()
        file_list = conn.execute("SELECT code, partner, run_id, \
            filename_pattern, cols_add, cols_del FROM file_run_status WHERE \
            run_id = ? AND partner = ?", (int(current_run_id), partner_code))
        do_once = True
        for item in file_list:
            # if we're doing a basic report, skip reporting on files where nothing changed
            if item['code'] == 1 and detailed == False:
                continue
            while do_once:
                report += partner_name['name_full'] +"\n"
                report += "-----------------------\n"
                do_once = False
            logger.debug("Partner %s %s ", partner_code, partner_name)
            logger.debug(item)
            report += get_file_status(logger, item['code'], item)

        do_once = True
        while do_once:
            report += "\n"
            do_once = False
    if detailed == True:
        report += "\nThis report generated by version {} of data_inbox.".format(VERSION_NUMBER)
    return report

def get_file_status(logger, code, item):
    """Helper function to return text describing what happened to a file."""
    if code == 1:
        return "{} has no change in header. OK to process.\n" \
            .format(item['filename_pattern'])
    elif code == 2:
        return "{} has a new column {}. Check before processing.\n" \
            .format(item['filename_pattern'], item['cols_add'])
    elif code == 3:
        return "{} is missing previously existing column(s) {}. " \
            "Check before processing.\n"\
            .format(item['filename_pattern'], item['cols_del'])
    elif code == 4:
        return "{} may be missing a header. No previous column names" \
            " matched. Check before processing.\n"\
            .format(item['filename_pattern'])
    #TODO does this ever get triggered?
    elif code == 5:
        return "{} is a new or unidentified filetype. Update \
            partners_filesets to match.\n"\
            .format(item['filename_pattern'])
    elif code == 6:
        return "{} has missing columns {} and new columns {}. Check" \
        " before processing.\n".format(item['filename_pattern'], \
        item['cols_del'], item['cols_add'])
    elif code == 7:
        return "No previous fileset stored for partner. " \
        "Header(s) have not been checked.\n"

def setup_tables(conn, logger):
    create_sql = input("Do you wish to create the needed SQL tables? (y/n) ")
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
    partner_list_query = ("SELECT id, name, name_full, incoming_file_directory,\
        tocheck FROM partners")
    partner_list = conn.execute(partner_list_query)
    for partner in partner_list:
        if partner['tocheck'] == "False":
            logger.info("%s is set to be not checked.", partner['name_full'])
            error_code = 4
            conn.execute('INSERT INTO partner_run_status \
                (code, partner, run_id) VALUES (?, ?, ?)', \
                (error_code, partner['id'], current_run_id))
            continue
        error_code = None
        logger.info("Now checking %s", partner['name_full'])
        if not os.path.isdir(partner['incoming_file_directory']):
            logger.error("Path %s not found for %s", \
                partner['incoming_file_directory'], partner['name_full'])
            error_code = 2  # directory missing
        elif not os.listdir(partner['incoming_file_directory']):
            logger.info("Path %s empty found for %s", \
                partner['incoming_file_directory'], partner['name_full'])
            error_code = 1  # no files, therefore no new files
        else:
            # verify that at least one actual file is in the directory
            all_dirs = True
            for item in os.listdir(partner['incoming_file_directory']):
                logger.debug("Now checking %s", item)
                if os.path.isfile(os.path.join(partner['incoming_file_directory'], item)):
                    all_dirs = False
            if all_dirs:
                error_code = 1
                logger.debug("all dirs for %s", partner)
            else:
                error_code = 3
                logger.debug("not all dirs for %s", partner)

        if error_code:
            conn.execute('INSERT INTO partner_run_status \
                (code, partner, run_id) VALUES (?, ?, ?)', \
                (error_code, partner['id'], current_run_id))
def make_partners_to_check_list(partner_info, conn, logger, current_run_id):
    """Generate list of partners to check"""
    partners_to_check = []
    report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=?", (int(current_run_id),))
    for row in report:
        #logger.debug(row)
        partner_name = partner_info[row['partner']]['name_full']
        partner_directory = partner_info[row['partner']]['incoming_file_directory']
        partner_id = partner_info[row['partner']]['id']
        if row['code'] == 1:
            pass # no new data
        if row['code'] == 2:
            pass # directory not found
        if row['code'] == 3:
            logger.info("Adding partner %s to the list to check", partner_name)
            partners_to_check.append({'id':partner_id, \
                'name_full':partner_name, 'dir':partner_directory})
    return partners_to_check

#def generate_violation_report(partner_info, conn, logger, current_run_id):
    # """Report if there are any violations."""
    # partners_to_check = []
    # list_of_partners_with_new = []
    # report = conn.execute("SELECT * FROM partner_run_status WHERE run_id=? and code=3", (int(current_run_id),))
    # # build list of partner ids to check
    # for row in report:
    #     list_of_partners_with_new.append(row['partner'])
    # for partner_code in list_of_partners_with_new:
    #     report = conn.execute("SELECT * FROM file_run_status WHERE run_id=? and partner=?", (int(current_run_id),int(partner_code)))
    # for row in report:
    #     print(row)

def check_partner_files(partner_info, conn, logger, current_run_id):
    """Iterate over files for each partner to check their headers."""
    logger.debug("Checking partner files")
    partners_to_check = make_partners_to_check_list(partner_info, conn, logger, current_run_id)
    # now check the list
    for partner in partners_to_check:
        name_full = partner['name_full']
        pid = partner['id']
        directory = partner['dir']
        logger.info("Now checking %s ", name_full)
        # load the previous fileset info
        #partner_fileset = load_previous_fileset(conn, pid, logger, name_full)
        new_fileset = os.listdir(directory)
        for new_file in new_fileset:
            partner_fileset = load_previous_fileset(conn, pid, logger, name_full)
            if not partner_fileset:
                try:
                    conn.execute("INSERT INTO file_run_status (code, partner, \
                    run_id, filename_pattern, filetype) \
                    VALUES (?, ?, ?, ?, ?)", (7, pid, current_run_id, \
                    new_file, "unknown"))
                    commit_tran(conn, logger)
                except:
                    logger.info("Unable to store file_run_status.")
            else:
                logger.debug("partner_fileset len: %i", len(partner_fileset))
            #logger.debug(partner_fileset)
            logger.info("Now checking new file %s", new_file)
            try:
                file_extension = new_file.split('.')[1].lower()
            except IndexError:
                logger.error("Unable to split file extension off file %s", new_file)
                logger.error("File will not be checked.")
                continue
            logger.debug("File extension: %s", file_extension)
            if file_extension in FILETYPES_TO_SKIP:
                logger.debug("Not checking %s because it is in the list of \
                    filetypes to skip.", new_file)
                continue
            # find a match
            # search the fileset to find a matching filename
            max_score = {'score': 0, 'filename': ""}
            for row in partner_fileset:
                # previous filename
                filename = row['filename_pattern']
                logger.debug("Current filename from fileset: %s", filename)
                # current filename check
                #logger.debug(filename)
                new_file_trim = new_file.split('.')[0]
                # ignore case in comparison
                new_file_trim_upper = new_file_trim.upper()
                new = new_file_trim_upper.translate({ord(k): None for k in digits})
                # get rid of numbers in the strings
                filename_upper = filename.upper()
                fname = filename_upper.translate({ord(k): None for k in digits})
                current_score = fuzz.ratio(new, fname)
                if current_score > max_score['score']:
                    max_score['score'] = current_score
                    max_score['filename'] = filename
                    max_score['header'] = row['header']
                    logger.debug("new max score found:")
                    logger.debug(max_score)

                logger.debug("Continuing to attempt to match %s", new_file_trim)
                #if new == fname or new in fname \
                #    or fuzz.ratio(new, fname) > MATCH_RATIO or new.find(fname) != -1:

            if max_score['score'] == 0:
                logger.info("new file: %s has no match", new_file_trim)
                conn.execute("INSERT INTO file_run_status (code, \
                    partner, run_id, filename_pattern, filetype) \
                    VALUES (?, ?, ?, ?, ?)", (5, pid, current_run_id, \
                    filename, "unknown"))
            else:
                filename = max_score['filename']
                cols = ""
                cols_add = ""
                cols_del = ""
                logger.info("Match found: %s", filename)
                status_and_cols = check_header(new_file, directory, max_score['header'], logger)
                status = status_and_cols[0]
                if len(status_and_cols) == 2:
                    if status == 2:
                        # there is a new column
                        cols_add = status_and_cols[1]
                    elif status == 3:
                        # there is a deleted column
                        cols_del = status_and_cols[1]
                elif len(status_and_cols) == 3:
                    cols_add = status_and_cols[1]
                    cols_del = status_and_cols[2]
                get_status(status, pid, current_run_id, new_file, cols, \
                    cols_add, cols_del, conn, logger)


def get_status(status, pid, current_run_id, new_file, cols, cols_add, \
            cols_del, conn, logger):
    """Given a status code, insert a row recording the file_run_status and log it."""
    if status == 1:
        logger.info("Storing exact match status for %s", new_file)
        conn.execute("INSERT INTO file_run_status (code, partner, \
            run_id, filename_pattern, filetype) VALUES (?, ?, ?, ?, ?)", \
            (1, pid, current_run_id, new_file, "unknown"))
        return
    elif status == 2:
        logger.info("Storing new column status for {}".format(new_file))
        conn.execute("INSERT INTO file_run_status (code, partner, \
            run_id, filename_pattern, filetype, cols_add) \
            VALUES (?, ?, ?, ?, ?, ?)", (2, pid, current_run_id, new_file, \
            "unknown", str(cols_add)))
        return
    elif status == 3:
        logger.info("Storing deleted column status for {}".format(new_file))
        conn.execute("INSERT INTO file_run_status (code, partner, \
            run_id, filename_pattern, filetype, cols_del) \
            VALUES (?, ?, ?, ?, ?, ?)", (3, pid, current_run_id, \
            new_file, "unknown", str(cols_del)))
        return
    elif status == 4:
        logger.info("Storing missing header status for {}".format(new_file))
        conn.execute("INSERT INTO file_run_status (code, partner, \
            run_id, filename_pattern, filetype) \
            VALUES (?, ?, ?, ?, ?)", (4, pid, current_run_id, new_file, "unknown"))
        return
    elif status == 6:
        logger.info("Storing missing and added col status status for {}"\
            .format(new_file))
        conn.execute("INSERT INTO file_run_status (code, partner, \
            run_id, filename_pattern, filetype, cols_add, cols_del) \
            VALUES (?, ?, ?, ?, ?, ?, ?)", (6, pid, current_run_id, new_file,\
            "unknown", str(cols_add), str(cols_del)))
    else:
        logger.info("something went wrong. status {}".format(status))

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

    logger.debug("Loading previous fileset for %s", name_full)
    if len(partner_fileset) == 0:
        logger.warning("No previous recorded fileset stored for %s", name_full)
        logger.error('%s will not be checked in this run.', name_full)
        return
    #else:
        #logger.debug(partner_fileset)
    return partner_fileset

def add_new_fileset(conn, logger):
    """Read files on disk and build a new fileset record for the partner."""
    partner_list = get_partner_list(conn, logger)
    filetype_dict = get_filetype_dict(conn, logger)
    logger.debug("Current list of partners")
    logger.debug(partner_list)
    for partner in partner_list:
        name = partner_list[partner]['name']
        directory = partner_list[partner]['stored_file_directory']
        try:
            list_of_dirs = os.listdir(directory)
        except:
            logger.error("Directory %s not found.", directory)
            continue
        if not list_of_dirs:
            logger.info("File directory empty for %s. Skipping building fileset.", name)
        else:
            logger.info("Now building fileset for %s", name)
            # iterate over newest first
            list_of_dirs = sorted(list_of_dirs, reverse=True)
            list_of_dirs_count = len(list_of_dirs)
            for new_dir in list_of_dirs:
                #logger.debug(new_dir)
                #logger.debug(os.path.isdir(os.path.join(directory + new_dir)))
                if os.path.isdir(os.path.join(directory + new_dir)):
                    #print("HERE")
                    get_out = input("There are {} remaining directories to scan " \
                        "for {}. Do you wish to continue? (Y/N)" \
                        .format(list_of_dirs_count, name))
                    list_of_dirs_count = list_of_dirs_count - 1
                    if get_out == 'N' or get_out == 'n':
                        break
                    new_dir = directory + new_dir
                    list_of_files = os.listdir(new_dir)
                    for new_file in list_of_files:
                        if os.path.isfile(os.path.join(new_dir, new_file)):
                            with open(os.path.join(new_dir, new_file), 'r') as f:
                                try:
                                    header = f.readline()
                                except UnicodeDecodeError:
                                    logger.info("UnicodeDecodeError: Unable to read header of file %s", new_file)
                                    continue
                            logger.info("Now trying to add file %s to fileset.", new_file)
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
        partner_dict[partner_key] = {'name': partner['name'], \
            'incoming_file_directory': partner['incoming_file_directory'], \
            'stored_file_directory': partner['stored_file_directory'], \
            'tocheck': partner['tocheck']}
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
        logger.info("Exact match found: %s looks like a %s file.", new_file, new_file.upper())
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
                logger.info("Partial match found for %s: looks like a %s file", new_file, item)
                add_to_filetype_dict(partner, filetype_id, new_file, header, conn, logger)
                file_matched = True
                break
    if not file_matched:
        logger.info("No full match found for %s. Not sure what it is.", new_file)
        logger.info("Adding with full filename.")
        add_to_filetype_dict(partner, 31, new_file, header, conn, logger)

def add_to_filetype_dict(partner, filetype_id, new_file, header, conn, logger):
    """Function to add a new file to the filetype dictionary."""
    logger.info("Adding new filetype record for partner %s for " \
        "filetype_id %i", partner, filetype_id)
    date_now = time.strftime('%Y-%m-%d %H:%M:%S')
    # get existing filetype record
    existing_filetype_row = conn.execute("SELECT * FROM partners_filesets \
        WHERE pid = ? AND filetype = ?", (partner, filetype_id))
    # partner can have multiple unknown, but can't have matching names
    try:
        existing_filetype_row = existing_filetype_row.fetchone()
    except:
        logger.debug("No previous record found.")
    if existing_filetype_row and int(filetype_id) != 31:
        logger.info("Existing filetype record found for partner %s and" \
            "filetype %i", partner, filetype_id)
        logger.info("Deleting previous filetype records from partners_filesets")
        delete_tran = conn.execute("DELETE FROM partners_filesets \
            WHERE pid = ? AND filetype = ?", (partner, filetype_id))
        commit_tran(conn, logger)
    elif existing_filetype_row and int(filetype_id) == 31 and \
            new_file == existing_filetype_row['filename_pattern']:
        logger.info("Matching file of unknown type with same name \
            already added for partner. Skipping.")
        return None
    else:
        logger.info("No existing filetype record found for partner %s \
            and filetype %i", partner, filetype_id)
    logger.info("Adding new filetype record.")
    conn.execute("INSERT INTO partners_filesets (pid, date, \
        filename_pattern, filetype, header) VALUES (?, ?, ?, ?, ?)", \
        (int(partner), date_now, new_file, filetype_id, header.strip()))
    commit_tran(conn, logger)
    #logger.warning("add_to_filetype_dict not yet implemented. NOOP")

def split_on_delim(line, delim):
    """Given a string and a delimiter, return the string split on the delimter."""
    return line.split(delim)

def find_delim_and_split(line, logger):
    """Given a string, try splitting it until you find the correct delimiter"""
    delim = ','
    line_split = split_on_delim(line, delim)
    if len(line_split) == 1:
        delim = ';'
        line_split = split_on_delim(line, delim)
        if len(line_split) == 1:
            delim = '\t'
            line_split = split_on_delim(line, delim)
            if len(line_split) == 1:
                logger.error("Couldn't find correct delimiter to split %s", line)
    return line_split, delim

def check_header(new_file, partner_directory, prev_header, logger):
    """Check new file header against previous header."""
    # TODO: make this work on non-exact matching
    logger.info("Now checking header for %s", new_file)
    with open(partner_directory + new_file, 'r') as f:
        header_row = f.readline().strip()

        logger.debug("Header for file %s:\n%s", new_file, header_row.strip())
        header_cols, delim = find_delim_and_split(header_row, logger)
        logger.debug("DELIMITER: %s", delim)
        prev_header = prev_header.split(delim)
        missing_header_cols = []
        #logger.debug(header_cols)
        #logger.debug(prev_header)
        starting_new_header_len = len(header_cols)
        # iterate over old header, deleting any cols in new header that match
        for old_col in prev_header:
            logger.debug("Column header trying to match: %s", old_col)
            try:
                header_cols.remove(old_col)
                logger.debug("Removed old column %s", old_col)
            except ValueError:
                logger.info("Old column %s is missing.", old_col)
                missing_header_cols.append(old_col)
        logger.debug("After matching columns: %i", len(header_cols))
        if len(header_cols) == 0 and len(missing_header_cols) == 0:
            logger.info("%s header exactly matches old header. No change in header.", new_file)
            return [1]
        elif len(header_cols) == starting_new_header_len:
            # if the length hasn't changed, assume header is missing in file
            logger.info("No columns from new header match old header.")
            logger.info("Header may have been deleted.")
            return [4]
        elif len(header_cols) != starting_new_header_len and \
            len(missing_header_cols) != 0 and len(header_cols) == 0:
            # at least some columns matched
            logger.info("Columns deleted.")
            #logger.debug("MISSING HEADER %s", missing_header_cols)
            return [3, missing_header_cols]
        elif len(header_cols) != 0 and len(missing_header_cols) != 0:
            # columns added and deleted
            logger.info("Columns added and deleted")
            return [6, header_cols, missing_header_cols]
        else:
            #TODO: trap here to check for addition
            logger.info("HEADER COLS: %s starting_new_header_len: %s missing_header_cols: %s", header_cols, starting_new_header_len, missing_header_cols)

            logger.info("%s header does not match old header.", new_file)
            logger.info("New column(s) found: %s", header_cols)
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
    file_handler_logger = logging.FileHandler('data_inbox.log')
    file_handler_logger.setLevel(logging.INFO)
    file_handler_logger.setFormatter(formatter)
    logger.addHandler(file_handler_logger)

    # logging to console
    console_handler_logger = logging.StreamHandler()
    if verbose:
        console_handler_logger.setLevel(logging.DEBUG)
    else:
        console_handler_logger.setLevel(logging.INFO)
    console_handler_logger.setFormatter(formatter)
    logger.addHandler(console_handler_logger)

    return logger


def send_report(report, from_address, to_address, mail_server):
    """Sends the report via email.
    Keyword arguments:
    report -- the report as a string.
    from_address -- email address the report will come from.
    to_address -- email address the report is going to.
    """
    #subj = "data_inbox report for " + str(datetime.datetime.now())
    #mail_string = "-s " + "'" + subj + "'" + " " + to_address + " < " + "'" + report + "'"
    #print(mail_string)
    #call(["mail", mail_string])
    #msg['Subject'] = \
    #    "data_inbox report for " + str(datetime.datetime.now()) + "\n"
    #msg['From'] = from_address
    #msg['To'] = to_address
    #os.system('subj=%', "'data_inbox report for ' + str(datetime.datetime.now()) + '\n'""
    #os.system("mail -s '$subj' BMI-DEVELOPERS@ad.ufl.edu < /dev/null
    #mail_connection = smtplib.SMTP(mail_server)
    #mail_connection.sendmail(from_address, to_address, msg.as_string())
    #mail_connection.quit()

def write_report(report, logger):
    """Write report out to a file so it can be sent later.
    """
    report_file_name = str(date.today()).replace('-', '') + "_report.txt"
    with open(report_file_name, 'w') as f:
        for row in report:
            f.write(row)
    logger.debug("Finished writing report out.")

if __name__ == '__main__':
    main()
