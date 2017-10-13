-- partner data file
INSERT INTO partners (name, name_full, incoming_file_directory, stored_file_directory, tocheck)
VALUES
 ('HCN', 'Health Choice Network', 'data/sftp/healthchoice/', 'data/inboxarchive/hcn/', 'True'),
 ('BND', 'Bond Clinic', 'data/sftp/bond/', 'data/inboxarchive/bond/', 'False'),
 ('UMI', 'University of Miami', 'data/sftp/umi/', 'data/inboxarchive/umi', 'True'),
 ('FLM', 'Institute of Child Health Policy (Medicaid)', 'data/sftp/ichp/', 'data/inboxarchive/ichp/', 'True'),
 ('CHP', 'Capital Health Plan', 'data/sftp/chp/', 'data/inboxarchive/chp/', 'False'),
 ('UFH', 'University of Florida Health', 'data/sftp/ufhealth/', 'data/inboxarchive/uf/', 'True'),
 ('TMH', 'Tallahassee Memorial Hospital', 'data/sftp/tmh/', 'data/inboxarchive/tmh/', 'True'),
 ('ORH', 'Orlando Health', 'data/sftp/oh/', 'data/inboxarchive/oh/', 'True'),
 ('FLH', 'Florida Hospital', 'data/sftp/floridahospital/', 'data/inboxarchive/flh/', 'True'),
 ('MCH', 'Miami Children''s Hospital', 'data/sftp/mch/', 'data/inboxarchive/mch', 'True');
