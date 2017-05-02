#!/usr/bin/env python

"""
fileset_db utility functions for maintaining the fileset database
"""

import logging

logger = logging.getLogger(__name__)

def create_empty_tables(conn):
    logger.warning('This will initialize the required tables.')
    required_tables = {
    'partners': """CREATE TABLE partners (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        name_full TEXT,
        file_directory TEXT
        )
        """,

    'partners_filesets': """ CREATE TABLE partners_filesets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATETIME,
        filename_pattern TEXT,
        header TEXT
    )
    """,

    'run_status': """CREATE TABLE run_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATETIME,
        filename TEXT,
        code INTEGER
    )
    """,

    'error_codes': """CREATE TABLE error_codes (
        id INTEGER PRIMARY KEY,
        error TEXT
    )
    """
    }

    for item in required_tables:
        logger.info("Excecuting {}".format(item))
        conn.execute(required_tables[item])
