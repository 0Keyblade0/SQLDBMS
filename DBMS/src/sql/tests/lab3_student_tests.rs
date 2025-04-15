use crate::sql::engine::Local;
use crate::sql::tests::utility::{create_storage_engine, SqlStudentRunner};
use itertools::Itertools;

const POLICE: &str = "police";
const STUDENT: &str = "student";
const TEST: &str = "test";

// ================================= Test Table =================================
const CREATE_TABLE_STATEMENT: &str = "CREATE TABLE test ( \
                id INT PRIMARY KEY, \
                \"bool\" BOOLEAN, \
                \"float\" FLOAT, \
                \"int\" INT, \
                \"string\" STRING \
            )";
const ROW1_INSERT: &str = "INSERT INTO test VALUES (1, true, 3.14, 7, 'foo')";
const ROW2_INSERT: &str = "INSERT INTO test VALUES (2, false, 2.718, 1, '👍')";
const TABLE_WITH_ROW1_AND_ROW2: &str = "test.id, test.bool, test.float, test.int, test.string ; \
                1, true, 3.14, 7, foo ; \
                2, false, 2.718, 1, 👍";
const EMPTY_TABLE: &str = "test.id, test.bool, test.float, test.int, test.string ;";

// ==============================================================================

#[test]
fn test_insert() {
    let storage_engine = create_storage_engine();
    let executor = Local::new(storage_engine);

    SqlStudentRunner::new(&executor)
        .execute(CREATE_TABLE_STATEMENT)
        .execute(ROW1_INSERT)
        .execute(ROW2_INSERT)
        .select_expect("SELECT * FROM test", TABLE_WITH_ROW1_AND_ROW2);
}

#[test]
fn test_insert_bulk() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);
    let bulk_insert = |runner: &mut SqlStudentRunner| {
        for _ in 0..1000 {
            [ROW1_INSERT, ROW2_INSERT].iter().for_each(|statement| {
                runner.execute(statement);
            })
        }
    };

    SqlStudentRunner::new(&engine)
        .execute(CREATE_TABLE_STATEMENT)
        // Insert a lot of rows...
        .bind(bulk_insert)
        // ...and check to make sure all those rows actually got inserted!
        .select_expect(
            "SELECT * FROM test",
            format!(
                "test.id, test.bool, test.float, test.int, test.string ; {}",
                (0..1000)
                    .map(|_| "1, true, 3.14, 7, foo ; 2, false, 2.718, 1, 👍")
                    .join(" ; ")
            )
            .as_str(),
        );
}

#[test]
fn test_select_constant() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    // Selecting a constant emits a single row with the constant field.
    SqlStudentRunner::new(&engine).select_expect("SELECT 42", " ; 42");
}

#[test]
fn test_select_constexpr() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    // Selecting constant expressions and constants emits a single row
    // with the evaluated expression values and constant values.
    SqlStudentRunner::new(&engine).select_expect(
        "SELECT NULL, NOT FALSE, 2^2+1, 3.14*2, 'Hi 👋'",
        ", , , , ; \
                NULL, true, 5, 6.28, Hi 👋",
    );
}

/// Tests some basic SELECT statements.
#[test]
fn test_select() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);
    let mut binding = SqlStudentRunner::new(&engine);

    // Intialize the table on which we'll be executing basic SELECT queries.
    let runner = binding.initialize(TEST);

    // A full table scan should emit every row.
    runner.select_expect("SELECT * FROM test", TABLE_WITH_ROW1_AND_ROW2);

    // SELECT on individual columns should emit just the value for that column.
    runner
        .select_expect("SELECT \"id\" FROM test", "test.id ; 1 ; 2")
        .select_expect("SELECT \"bool\" FROM test", " test.bool ; true; false")
        .select_expect("SELECT \"float\" FROM test", "test.float ; 3.14 ; 2.718")
        .select_expect("SELECT \"int\" FROM test", "test.int ; 7 ; 1")
        .select_expect("SELECT \"string\" FROM test", "test.string ; foo ; 👍");
}

#[test]
fn test_scan_with_join() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    SqlStudentRunner::new(&engine)
        // "test" table schema: INT, BOOL, FLOAT, INT, STRING
        .initialize(TEST)
        // "other" table schema: INT, STRING
        .execute("CREATE TABLE other (id INT PRIMARY KEY, value STRING)")
        .execute("INSERT INTO other VALUES (1, 'a'), (2, 'b')")
        // scan over a cross join between "test" and "other"
        .select_expect(
            "SELECT \"bool\", value FROM test, other",
            "test.bool, other.value; \
                          true, a; \
                          true, b; \
                          false, a; \
                          false, b",
        );
}

#[test]
fn test_where() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    let mut binding = SqlStudentRunner::new(&engine);

    // Initialize tables used in the test:
    let runner = binding
        // "first" table schema: INT, STRING
        .execute("CREATE TABLE first (id INT, value STRING)")
        .execute("INSERT INTO first VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        // "other" table schema: INT, BOOLEAN
        .execute("CREATE TABLE other (id INT PRIMARY KEY, \"bool\" BOOLEAN)")
        .execute("INSERT INTO other VALUES (1, FALSE), (2, TRUE)");

    // TRUE and FALSE filters work as expected (NULL is evaluated like FALSE):
    runner
        .select_expect(
            "SELECT * FROM first WHERE TRUE",
            "first.id, first.value ; \
                          1 , a ; \
                          2, b ;\
                          3, c",
        )
        .select_expect(
            "SELECT * FROM first WHERE FALSE",
            "first.id, first.value ; ",
        )
        .select_expect("SELECT * FROM first WHERE NULL", "first.id, first.value ; ");

    // Field predicate expressions work as expected.
    runner
        .select_expect(
            "SELECT * FROM first WHERE id > 1",
            "first.id, first.value ; \
                          2, b ; \
                          3, c",
        )
        .select_expect(
            "SELECT * FROM first WHERE id > 1 AND value < 'c'",
            "first.id, first.value ; \
                         2, b",
        );

    // WHERE predicate works on joined tables.
    runner
        // Unambiguous columns work.
        .select_expect(
            "SELECT * FROM first, other WHERE value = 'b'",
            "first.id, first.value, other.id, other.bool ; \
                        2, b, 1, false ; \
                        2, b, 2, true ",
        )
        // WHERE can be combined with joins, even when aliased.
        .select_expect(
            "SELECT * FROM first t JOIN other o ON t.id = o.id WHERE t.id > 1",
            "first.id, first.value, other.id, other.bool ; \
                      2, b, 2, true",
        );
}

#[test]
fn test_limit() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    SqlStudentRunner::new(&engine)
        .execute("CREATE TABLE limited (id INT, value STRING)")
        .execute("INSERT INTO limited VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        // The LIMIT clause should restrict the number of emitted tuples, as desired.
        .select_expect(
            "SELECT * FROM limited LIMIT 2",
            "limited.id, limited.value ; \
                          1, a ;\
                          2, b",
        )
        // Limits can be expressions, but only constant ones.
        .select_expect(
            "SELECT * FROM limited LIMIT 1 + 1",
            "limited.id, limited.value ; \
                          1, a ; \
                          2, b",
        );
}

#[test]
fn test_aggregate_constants() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    // COUNT works on constant values.
    SqlStudentRunner::new(&engine).select_expect(
        "SELECT COUNT(NULL), COUNT(TRUE), COUNT(1), COUNT(3.14), COUNT(NAN), COUNT('')",
        " , , , , , ; 0, 1, 1, 1, 1, 1",
    );
}

/// Note: this does NOT test AVG or MIN functionality (nor does it comprehensively cover
/// the functionality of the aggregations tested). As always, this test passing does not
/// guarantee the correctness of the operator implementation, and you should write some
/// more thorough tests!
#[test]
fn test_aggregate_basic() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);

    // Instantiate the table we're going to be testing aggregations over.
    let mut binding = SqlStudentRunner::new(&engine);
    let runner = binding
        .execute(
            "CREATE TABLE test ( \
                id INT PRIMARY KEY, \
                \"bool\" BOOLEAN, \
                \"int\" INTEGER, \
                \"float\" FLOAT, \
                \"string\" STRING, \
                static INT \
            )",
        )
        .execute("INSERT INTO test VALUES (0, TRUE,   100,    1.111,     'red', 1)")
        .execute("INSERT INTO test VALUES (1, FALSE,  0,      3.14,      'a',   1)")
        .execute("INSERT INTO test VALUES (2, TRUE,  -1,     -2.718,     'ab',  1)")
        .execute("INSERT INTO test VALUES (3, FALSE,  1,      0.0,       'aaa', 1)")
        .execute("INSERT INTO test VALUES (4, FALSE,  1000,  -0.1,       'A',   1)")
        .execute("INSERT INTO test VALUES (5, FALSE, -1000,   INFINITY,  '',    1)")
        .execute("INSERT INTO test VALUES (6, FALSE,  7,     -INFINITY,  'åa',  1)")
        .execute("INSERT INTO test VALUES (7, FALSE, -9,      NAN,       'Åa',  1)")
        .execute("INSERT INTO test VALUES (8, FALSE, -7890,   1.2345,    'B',   1)")
        .execute("INSERT INTO test VALUES (9, FALSE,  42,    -2.4690,    '👋',  1)");

    // Tests basic COUNT functionality:
    // - COUNT(*) returns the row count.
    // - COUNT works on no rows.
    // - COUNT returns number of non-NULL values.
    runner
        .select_expect("SELECT COUNT(*) FROM test", " ; 10")
        .select_expect(
            "SELECT COUNT(id), COUNT(\"bool\"), COUNT(\"float\"), COUNT(\"string\") \
                    FROM test WHERE false",
            " , , , ; 0, 0, 0, 0",
        )
        .select_expect(
            "SELECT COUNT(id), COUNT(\"bool\"), COUNT(\"float\"), COUNT(\"string\") \
                        FROM test",
            " , , , ; 10, 10, 10, 10",
        );

    // Tests basic MAX functionality:
    // - MAX returns the max value, or NULL if any value is NULL.
    runner
        .select_expect("SELECT MAX(id) FROM test", " ; 9")
        .select_expect("SELECT MAX(\"bool\") FROM test", " ; true")
        .select_expect("SELECT MAX(\"int\") FROM test", " ; 1000")
        .select_expect("SELECT MAX(\"float\") FROM test", " ; NaN")
        .select_expect(
            "SELECT MAX(\"float\") FROM test WHERE \"float\" IS NOT NAN",
            " ; inf",
        );

    // Tests basic SUM functionality:
    // - SUM works on constant values, but only numbers.
    // - SUM works on no rows.
    // - SUM returns the sum, or NULL if any value is NULL.
    runner
        .select_expect(
            "SELECT SUM(NULL), SUM(1), SUM(3.14), SUM(NAN) FROM test",
            " , , , ; NULL, 10, 31.399998, NaN",
        )
        .select_expect(
            "SELECT SUM(id), SUM(\"bool\"), SUM(\"float\"), SUM(\"string\") \
                            FROM test WHERE false",
            " , , , ; NULL, NULL, NULL, NULL",
        )
        .select_expect(
            "SELECT SUM(id) FROM test",
            format!(" ; {}", (0..=9).sum::<i32>()).as_str(),
        )
        .select_expect("SELECT SUM(\"int\") FROM test", " ; -7750")
        .select_expect("SELECT SUM(\"float\") FROM test", " ; NaN");

    // A couple of funny edge cases:
    // - Constant aggregates can be used with rows.
    // - Repeated aggregates work.
    // - Aggregate can be expression, both inside and outside the aggregate.
    runner
        .select_expect(
            "SELECT COUNT(1), MIN(1), MAX(1), SUM(1), AVG(1) FROM test",
            " , , , , ; 10, 1, 1, 10, 1",
        )
        .select_expect(
            "SELECT MAX(\"int\"), MAX(\"int\"), MAX(\"int\") FROM test",
            " , , ; 1000, 1000, 1000",
        )
        .select_expect(
            "SELECT SUM(\"int\" * 10) / COUNT(\"int\") + 7 FROM test WHERE \"int\" IS NOT NULL",
            " ; -7743",
        );
}

#[test]
fn test_delete() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);
    let mut binding = SqlStudentRunner::new(&engine);

    let populate = |runner: &mut SqlStudentRunner| {
        runner
            .execute(ROW1_INSERT)
            .execute(ROW2_INSERT)
            .select_expect("SELECT * FROM test", TABLE_WITH_ROW1_AND_ROW2);
    };

    // Initialize the table from which we'll delete rows:
    let runner = binding.execute(CREATE_TABLE_STATEMENT);

    // Insert rows, and then delete them.
    runner
        .bind(populate)
        .execute("DELETE FROM test")
        .select_expect("SELECT * FROM test", EMPTY_TABLE);

    // Trivial filter should work.
    runner
        .bind(populate)
        .execute("DELETE FROM test WHERE false")
        .select_expect("SELECT * from test", TABLE_WITH_ROW1_AND_ROW2)
        .execute("DELETE FROM test WHERE true")
        .select_expect("SELECT * FROM test", EMPTY_TABLE);

    // Delete by key lookup
    runner
        .bind(populate)
        .execute("DELETE FROM test WHERE id = 1 OR id = 3")
        .select_expect(
            "SELECT * FROM test",
            "test.id, test.bool, test.float, test.int, test.string ; 2, false, 2.718, 1, 👍",
        );
}

#[test]
fn test_update_expression() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);
    let mut binding = SqlStudentRunner::new(&engine);
    fn with_schema(rows: &str) -> String {
        format!("test.id, test.value, test.quantity ; {}", rows)
    }

    // Initialize table that we'll be updating:
    let runner = binding
        .execute("CREATE TABLE test (id INT PRIMARY KEY, value INT, quantity INT NOT NULL)")
        .execute("INSERT INTO test VALUES (0, NULL, 0), (1, 1, 0), (2, 2, 0)")
        .select_expect(
            "SELECT * FROM test",
            &with_schema("0, 0, 0 ; 1, 1, 0 ; 2, 2, 0"),
        );

    // UPDATE can evaluate constant expressions
    runner
        .execute("UPDATE test SET value = 2 * 2 + 3")
        .select_expect(
            "SELECT * FROM test",
            &with_schema("0, 7, 0 ; 1, 7, 0 ; 2, 7, 0"),
        );

    // UPDATE can evaluate variable expressions.
    runner
        .execute("UPDATE test SET value = id + 10 - quantity")
        .select_expect(
            "SELECT * FROM test",
            &with_schema("0, 10, 0 ; 1, 11, 0 ; 2, 12, 0"),
        );

    // UPDATE evaluation uses the old values.
    runner
        .execute("UPDATE test SET value = id + 1, quantity = value")
        .select_expect(
            "SELECT * FROM test",
            &with_schema("0, 1, 10 ; 1, 2, 11 ; 2, 3, 12"),
        );
}

#[test]
fn test_update_where() {
    let storage_engine = create_storage_engine();
    let engine = Local::new(storage_engine);
    let mut binding = SqlStudentRunner::new(&engine);

    // We don't have transactional rollbacks yet; let's simulate it here >.<
    let rollback = |runner: &mut SqlStudentRunner| {
        runner
            .execute("DELETE from name")
            .execute("INSERT INTO name VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 4);");
    };

    // Initialize table that we'll be updating:
    let runner = binding
        .execute("CREATE TABLE name (id INT PRIMARY KEY, value STRING, \"index\" INT INDEX)")
        .execute("INSERT INTO name VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 4);");

    // Boolean filters work, and are trivial.
    runner
        .execute("UPDATE name SET value = 'foo' WHERE TRUE")
        .select_expect(
            "SELECT * FROM name",
            "name.id, name.value, name.index ; \
                      1, foo, 1 ; \
                      2, foo, 2 ; \
                      3, foo, 4",
        )
        .bind(rollback)
        .execute("UPDATE name SET value = 'foo' WHERE FALSE")
        .select_expect(
            "SELECT * FROM name",
            "name.id, name.value, name.index ; \
                            1, a, 1 ; \
                            2, b, 2 ; \
                            3, c, 4",
        )
        .bind(rollback);

    // Updating by primary key lookup.
    runner
        .execute("UPDATE name SET value = 'foo' WHERE id = 1 OR id = 3")
        .select_expect(
            "SELECT * FROM name",
            "name.id, name.value, name.index ; \
                            2, b, 2 ; \
                            1, foo, 1 ;\
                            3, foo, 4",
        )
        .bind(rollback);

    // Updating by arbitrary predicate over full scan.
    runner
        .execute("UPDATE name SET value = 'foo' WHERE id >= 5 - 2 OR (value LIKE 'a') IS NULL")
        .select_expect(
            "SELECT * FROM name",
            "name.id, name.value, name.index ; \
                            1, a, 1 ; \
                            2, b, 2 ; \
                            3, foo, 4",
        );
}

#[test]
fn test_setup_police() {
    let engine = Local::new(create_storage_engine());
    SqlStudentRunner::new(&engine)
        .initialize(POLICE)
        .select_expect(
            "SELECT * FROM data_officerallegation a JOIN data_officer o ON a.officer_id = o.id",
            "data_officerallegation.id, data_officerallegation.crid, \
                    data_officerallegation.officer_id, data_officerallegation.allegation_description, \
                    data_officer.id, data_officer.first_name, data_officer.last_name, \
                    data_officer.birth_year, data_officer.appointment_year, data_officer.gender, \
                    data_officer.race ;
                0, 101, 2, Neglecting to follow proper arrest procedures., 2, Jane, Smith, 1985, 2010, F, Black ; \
                1, 102, 4, Racial profiling incident., 4, Emily, Brown, 1990, 2012, F, Asian ; \
                2, 103, 5, Unlawful search during vehicle stop., 5, Robert, Williams, 1978, 2003, M, White ; \
                3, 104, 3, Failure to report misconduct by fellow officer., 3, Mark, Johnson, 1975, 2000, M, Hispanic ; \
                4, 105, 2, Retaliation against a civilian following a complaint., 2, Jane, Smith, 1985, 2010, F, Black ; \
                5, 101, 5, Failure to document use of force incident., 5, Robert, Williams, 1978, 2003, M, White ; \
                6, 102, 1, Use of excessive force during a traffic stop., 1, John, Doe, 1980, 2005, M, White ; \
                7, 103, 3, Inappropriate conduct during civilian questioning., 3, Mark, Johnson, 1975, 2000, M, Hispanic",
        );
}

#[test]
fn test_setup_student() {
    let engine = Local::new(create_storage_engine());
    SqlStudentRunner::new(&engine)
        .initialize(STUDENT)
        .select_expect(
            "SELECT s.netid, s.gpa, c.name FROM student s \
                            JOIN enroll e ON s.id = e.student_id \
                            JOIN course c ON c.id = e.course_id",
            "student.netid, student.gpa, course.name ; \
                        abc1234, 3.85, Introduction to Computer Science ; \
                        abc1234, 3.85, Data Structures and Algorithms ; \
                        xyz5678, 3.75, Introduction to Computer Science ; \
                        xyz5678, 3.75, Database Systems ; \
                        lmn9012, 3.9, Operating Systems ; \
                        lmn9012, 3.9, Discrete Mathematics ; \
                        pqr3456, 3.65, Introduction to Computer Science ; \
                        pqr3456, 3.65, Operating Systems ; \
                        stu7890, 3.4, Database Systems ;",
        )
        .select_expect(
            "SELECT s.netid, s.gpa, c.name FROM student s \
                            JOIN enroll e ON s.id = e.student_id \
                            JOIN course c ON c.id = e.course_id \
                        ORDER BY s.gpa DESC",
            "student.netid, student.gpa, course.name ; \
                        lmn9012, 3.9, Operating Systems ; \
                        lmn9012, 3.9, Discrete Mathematics ; \
                        abc1234, 3.85, Introduction to Computer Science ; \
                        abc1234, 3.85, Data Structures and Algorithms ; \
                        xyz5678, 3.75, Introduction to Computer Science ; \
                        xyz5678, 3.75, Database Systems ; \
                        pqr3456, 3.65, Introduction to Computer Science ; \
                        pqr3456, 3.65, Operating Systems ; \
                        stu7890, 3.4, Database Systems ;",
        );
}
