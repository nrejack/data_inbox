--file error codes data file
INSERT INTO file_error_codes (id, error)
VALUES
  (1, 'No change in header'),
  (2, 'New column'),
  (3, 'Column deleted'),
  (4, 'Header missing'),
  (5, 'New filename pattern'),
  (6, 'Column(s) deleted and column(s) added');
