CREATE TABLE test (
    id INT PRIMARY KEY,
    "bool" BOOL,
    "float" FLOAT,
    "int" INT,
    "string" STRING);

INSERT INTO test VALUES (1, true, 3.14, 7, 'foo');

INSERT INTO test VALUES (2, false, 2.718, 1, '👍');