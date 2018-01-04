=========
Changelog
=========

Version 0.3
===========
Summary: Added log rotation to prevent them from growing too large. New and old
headers can have different delimiters.

Bumping version number to 0.3
When comparing headers, new and old headers can have different delimiters now.
Adding placeholder initial log file in log/ directory.
Changed default log file size to 15 MB.
Write logs to new log/ directory
Changed file logger to rotate based on size.
Simplify logging in partner_report function.
No longer try to check directories one level down in partner directories.
Adjusting debug messages to log only if in verbose mode.
Adding fileset_db* to .gitignore.
Adding 0.2 release notes text file.
Merge branch 'master' of https://bmi.program.ufl.edu/bitbucket/scm/on/data_inbox
Drop microseconds from datetime in report.
Fixing so newline not appended when running exception report.
Added message that appears when no exceptions are noted.
Corrected error where file has 0 match. Now stores actual name of file on disk.
Add extra newline when reporting exceptions.
Merge tag '0.2' into develop
Updating CHANGES.rst for 0.2 release.

Version 0.2
===========
Summary: Improved reporting: exceptions are now highlighted at the top of the
report. Bugfixes from problems encountered while matching new filetypes.

Added exception trapping for when a non-text file that is not on the blacklist of filetypes appears.
When no exceptions are found on a run, report reflects this.
Clean up formatting of log messages while building fileset.
Improved ability to skip directories when buildling fileset.
Modified delimiter checking code and added | as option
Modifying reporting.
Clean up spacing in report.
Added exception reporting to top of report.
Corrected deleted columns reporting- now shows deleted previous columns.
Adding version number to final report.
Adding changelog as CHANGES.rst
Reducing volume of logger messages.
Updates to README after initial deployment.
Adding missing underscore to report name
report.txt now has date appended to filename
Write out report to report.txt.
Changing send_report to use unix mail
Write correct filename to file_run_status
Verify that a directory does not have just directories inside it when checking for new files.
Don't try and split filenames if it's a directory.
Check whether file is a directory when scanning header.
Catching UnicodeDecodeError exception.
Removing unnessary print statement.
When scanning directories, make sure to check the full path.
Fixed another early exit.
Removed early exit before all sql files are read in.
Fixing typos.
data_inbox/data_inbox.py
Fix to not attempt to scan files that are not directories.
Merge tag '0.1' into develop

Version 0.1
===========
Summary: initial release with ability to build fileset records and scan incoming
files to check headers against the fileset records.

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
