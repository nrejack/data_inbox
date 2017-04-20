#!/usr/bin/env python

"""
data_inbox: data quality checker
"""

import logging
import sqlite3
import shutil
import click

FILESET_DATABASE = 'fileset_db.sqlite'
FILESET_DATABASE_BACKUP = 'fileset_db.sqlite.bk'

@click.option('-v', '--verbose', help='Run in verbose mode.', is_flag=True, default=False)
@click.command()
def main(verbose):
    logger = configure_logging(verbose)

    logger.info("Starting data_inbox.py")
    # open database
    logger.info("Backing up database {} to {}".format(FILESET_DATABASE, FILESET_DATABASE_BACKUP))
    shutil.copy2(FILESET_DATABASE, FILESET_DATABASE_BACKUP)
    logger.info("Opening database: {}".format(FILESET_DATABASE))
    conn = sqlite3.connect(FILESET_DATABASE)
    c = conn.cursor()

    logger.info("Committing changes to {}".format(FILESET_DATABASE))
    conn.commit()
    logger.info("Closing {}".format(FILESET_DATABASE))
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
