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
        filetype INTEGER,
        header TEXT,
        FOREIGN KEY (filetype) REFERENCES filetypes(filetype_id)
    )
    """,

    'run_status': """CREATE TABLE run_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATETIME,
        filename TEXT,
        code INTEGER,
        partner INTEGER,
        FOREIGN KEY (partner) REFERENCES partners(id)
    )
    """,

    'partner_error_codes': """CREATE TABLE partner_error_codes (
        id INTEGER PRIMARY KEY,
        error TEXT
    )
    """,

    'file_error_codes': """CREATE TABLE file_error_codes (
        id INTEGER PRIMARY KEY,
        error TEXT
    )
    """,
    
    'filetypes': """CREATE TABLE filetypes (
        filetype_id INTEGER PRIMARY KEY,
        filetype_name TEXT
    )
    """
    }

    for item in required_tables:
        logger.info("Executing {}".format(item))
        conn.execute(required_tables[item])
