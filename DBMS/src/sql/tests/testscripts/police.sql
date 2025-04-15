CREATE TABLE data_officer (
       id INT PRIMARY KEY,
       first_name STRING,
       last_name STRING,
       birth_year INT,
       appointment_year INT,
       gender STRING,
       race STRING);

CREATE TABLE data_allegation (
   crid INT PRIMARY KEY,
   latitude FLOAT,
   longitude FLOAT,
   is_officer_complaint BOOL,
   address STRING);

CREATE TABLE data_officerallegation (
       id INT PRIMARY KEY,
       crid INT,
       officer_id INT,
       allegation_description STRING);

INSERT INTO data_officer (id, first_name, last_name, birth_year, appointment_year, gender, race)
VALUES
    (1, 'John', 'Doe', 1980, 2005, 'M', 'White'),
    (2, 'Jane', 'Smith', 1985, 2010, 'F', 'Black'),
    (3, 'Mark', 'Johnson', 1975, 2000, 'M', 'Hispanic'),
    (4, 'Emily', 'Brown', 1990, 2012, 'F', 'Asian'),
    (5, 'Robert', 'Williams', 1978, 2003, 'M', 'White');

INSERT INTO data_allegation (crid, latitude, longitude, is_officer_complaint, address)
VALUES
    (101, 41.8781, -87.6298, TRUE, '1234 W Washington Blvd, Chicago, IL'),
    (102, 41.8500, -87.6500, FALSE, '5678 S State St, Chicago, IL'),
    (103, 41.9000, -87.7000, TRUE, '9101 N Ashland Ave, Chicago, IL'),
    (104, 41.7500, -87.5800, FALSE, '3345 E 75th St, Chicago, IL'),
    (105, 41.8200, -87.6800, TRUE, '9807 W Madison St, Chicago, IL');

INSERT INTO data_officerallegation (crid, officer_id, allegation_description)
VALUES
    (0, 101, 2, 'Neglecting to follow proper arrest procedures.'),
    (1, 102, 4, 'Racial profiling incident.'),
    (2, 103, 5, 'Unlawful search during vehicle stop.'),
    (3, 104, 3, 'Failure to report misconduct by fellow officer.'),
    (4, 105, 2, 'Retaliation against a civilian following a complaint.'),
    (5, 101, 5, 'Failure to document use of force incident.'),
    (6, 102, 1, 'Use of excessive force during a traffic stop.'),
    (7, 103, 3, 'Inappropriate conduct during civilian questioning.'),
    (8, 104, 2, 'Failure to de-escalate a potentially dangerous situation.'),
    (9, 105, 4, 'Misuse of police authority to access personal records.');