=========
Changelog
=========

Version 0.1
===========
Merge branch 'release/0.1'
Fixed logic that stores the status when columns have been both deleted and added.
Additional cleanup from pylint, new score 8.73/10
Adding python-Levenshtein for faster matching.
Fixing logger statements after linting.
Minor pylint fixes.
Improves reporting when there is no previous fileset stored for partner.
Modifies reporting to only print categories that have entries.
Changes code that builds fileset records to scan newest first.
Removed email addresses used for testing.
Improves documentation in README.rst.
Moves sql files to sql/ directory.
Adds filetypes for HCN partner as they do not follow standard naming scheme.
Improves code that builds fileset for each partner.
Adding ability to skip checking specific filetypes.
Modifies filetype guessing algorithm to check against all files, and pick the one with the best match ratio.
Adds smarter filetype and name matching after running on FLH data.
Changes flag so that not creating tables fresh is the default behavior
Adds support to report missing and added columns.
Fixes loop error where files were being reported as not matching.
Adds code that generates individual file-level reports.
Adds code to properly generate and store error codes showing what happened to a file's header.
Update README and requirements, first pass.
Adds code to send report via email."
Adds working function to scan directories and add filetypes headers.
Adds code to determine filetype of new files.
Adds updated requirements.txt.
Refactors partner fileset loading.
Various fixes after linting.
Adding comments to SQL code explaining usage of the tables.
Adding working basic version of check_header.
Adds initial framework for checking the header in a partner's files.
Adds fixes after some variable name changes.
Multiple corrections to improve pylint score.
Adds function to do basic reporting on the status of the partners' directories.
Adds basic checking for whether partner's directory is empty or exists.
Adds shell for basic reporting function (print in console.)
Adds new table that lists the different filetypes.
Adds utility for creating database.
Adds SQL file with list of error codes.
Adds code to skip creating new tables and load data into them.
Adds code to read file into database. Not completed.
Adds new code to interact with database.
Refactor to move logging into separate function.
Adds shell of main program with logging.
Initial commit after PyScaffold project creation.
