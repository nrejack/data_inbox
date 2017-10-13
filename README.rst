==========
data_inbox
==========


data_inbox is a tool designed to check the headers of incoming .csv files and
report on any changes over time.


Getting started
===============
- Clone the repository. The repo comes with all the code and data needed to get started.
- Use virtualenvwrapper to install.

::

  mkvirtualenv data_inbox -p /usr/local/bin/python3
  pip install -r requirements.txt

- data_inbox tracks information about the partner filesets in a sqlite database.
  Several .sql files are stored in the sql/ directory that contain information about your environment to get you started.
- If needed, edit the partner_data.sql file to specify the correct file locations.
- Run the program with the 'create' option to create and load the initial tables. The 'verbose' option is encouraged:

::

  python data_inbox/data_inbox.py --create --verbose

- Now that the tables are created, we need to read the existing filesets for each partner to store the historical headers to check against.

 ::

  python data_inbox/data_inbox.py --buildfileset --verbose

- The program will iterate over the directories (as defined in the stored_file_directory in partners.sql), attempt to determine the filetype, and store the fileset for the partner in the database.
- Now that setup is complete, you can use the program to regularly check incoming files. It is recommended to cron the program to run once daily.
- data_inbox will scan the incoming_file_directory as defined in partners.sql, try to guess the filetype of incoming files, and try to match the headers.
- data_inbox will attempt to determine whether the header has changed, including whether columns are added or deleted.
- WARNING: If partners change the name of a file too much, or send files with similar names, the attempt to match the filetype and header may fail and report false positives. Do not use data_inbox as a substitute for proper business rules involving honest brokers.


Description
===========
Python3 is recommended.


Note
====

This project has been set up using PyScaffold 2.5.7. For details and usage
information on PyScaffold see http://pyscaffold.readthedocs.org/.
