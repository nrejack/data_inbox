#!/usr/bin/env python

"""
fileset_db utility functions for maintaining the fileset database
"""

import logging

logger = logging.getLogger(__name__)

def create_empty_tables(conn):
    """Create the table structure required for the data model."""
    logger.warning('This will initialize the required tables.')
    required_tables = {
        'partners': """
        -- this table stores the partners, their names, and the location
        -- of their files
        CREATE TABLE partners (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        name_full TEXT,
        file_directory TEXT
        )
        """,

        'partners_filesets': """
        -- this table stores the headers of previous files for each partner,
        -- and identifies what type of file each is
        CREATE TABLE partners_filesets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pid INTEGER,
        date DATETIME,
        filename_pattern TEXT,
        filetype INTEGER,
        header TEXT,
        FOREIGN KEY (filetype) REFERENCES filetypes(filetype_id)
        FOREIGN KEY (pid) REFERENCES partners(id)
        )
        """,

        'partner_run_status': """

        CREATE TABLE partner_run_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code INTEGER,
        partner INTEGER,
        run_id INTEGER,
        FOREIGN KEY (partner) REFERENCES partners(id),
        FOREIGN KEY (run_id) REFERENCES current_run_status(id)
        )
        """,

        'current_run_status': """CREATE TABLE current_run_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date DATETIME
        )
        """,

        'file_run_status': """CREATE TABLE file_run_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code INTEGER,
        partner INTEGER,
        run_id INTEGER,
        filename_pattern TEXT,
        filetype INTEGER,
        cols_add TEXT,
        cols_del TEXT,
        FOREIGN KEY (code) REFERENCES file_error_codes(id),
        FOREIGN KEY (partner) REFERENCES partners(id),
        FOREIGN KEY (run_id) REFERENCES current_run_status(id),
        FOREIGN KEY (filetype) REFERENCES filetype(filetype_id)
        )
        """,

        'partner_error_codes': """
        -- this table stores the error codes at the partner level
        CREATE TABLE partner_error_codes (
        id INTEGER PRIMARY KEY,
        error TEXT
        )
        """,

        'file_error_codes': """
        -- this table stores the error codes for file-level errors
        CREATE TABLE file_error_codes (
        id INTEGER PRIMARY KEY,
        error TEXT
        )
        """,

        'filetypes': """
        -- this table stores the different type of files used in the PCORI CDM
        CREATE TABLE filetypes (
        filetype_id INTEGER PRIMARY KEY,
        filetype_name TEXT
        )
        """
    }

    for item in required_tables:
        logger.info("Executing {}".format(item))
        conn.execute(required_tables[item])
