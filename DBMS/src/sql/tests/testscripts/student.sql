CREATE TABLE student (
  id INT PRIMARY KEY,
  netid TEXT,
  first_name TEXT,
  last_name TEXT,
  class_year INT,
  gpa FLOAT);

CREATE TABLE course (
  id INT PRIMARY KEY,
  name TEXT,
  quarter TEXT,
  room TEXT
);

CREATE TABLE enroll (
   student_id INT,
   course_id INT,
   grade FLOAT
);

INSERT INTO student (id, netid, first_name, last_name, class_year, gpa)
VALUES
    (1, 'abc1234', 'John', 'Doe', 2024, 3.85),
    (2, 'xyz5678', 'Jane', 'Smith', 2025, 3.75),
    (3, 'lmn9012', 'Alice', 'Johnson', 2026, 3.90),
    (4, 'pqr3456', 'Bob', 'Brown', 2024, 3.65),
    (5, 'stu7890', 'Charlie', 'Miller', 2023, 3.40);

INSERT INTO course (id, name, quarter, room)
VALUES
    (101, 'Introduction to Computer Science', 'Fall', 'Room 101'),
    (102, 'Data Structures and Algorithms', 'Winter', 'Room 202'),
    (103, 'Operating Systems', 'Spring', 'Room 303'),
    (104, 'Discrete Mathematics', 'Fall', 'Room 101'),
    (105, 'Database Systems', 'Winter', 'Room 204');

INSERT INTO enroll (student_id, course_id, grade)
VALUES
    (1, 101, 3.90),
    (1, 102, 3.85),
    (2, 101, 3.80),
    (2, 105, 3.70),
    (3, 103, 4.00),
    (3, 104, 3.95),
    (4, 101, 3.60),
    (4, 103, 3.70),
    (5, 105, 3.40);