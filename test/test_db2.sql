-- Creating a user in DB2 (DB2 does not support CREATE USER directly)
CREATE SCHEMA pgctest;

-- Granting tablespace and other privileges is handled via the administrative commands in DB2
GRANT USE OF TABLESPACE USERSPACE1 TO USER pgctest;
GRANT CONNECT ON DATABASE TO USER pgctest;

-- Creating the Emp table in DB2
CREATE TABLE pgctest.Emp
(
    EID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    FIRST_NAME VARCHAR(40),
    LAST_NAME VARCHAR(40),
    EMAIL VARCHAR(100),
    HIRE_DT DATE,
    AGE INTEGER,
    "Zip" INTEGER,
    STATUS CHAR(3),
    SALARY DECIMAL(12,2),
    LAST_LOGIN TIMESTAMP,
    BIO VARCHAR(2000)
);

-- Inserting data into Emp table (DB2 uses ISO standard formats for DATE and TIMESTAMP)
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (1, 'John', 'Doe', 'john.doe@example.com', '2022-01-15', 30, 90210, 'ACT', 60000.00, '2024-08-20 09:30:00', 'Software Engineer with 5 years of experience.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', '2021-05-23', 28, 10001, 'ACT', 65000.00, '2024-08-19 14:45:00', 'Data Scientist specializing in machine learning.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (3, 'Alice', 'Johnson', 'alice.johnson@example.com', '2020-11-12', 34, 30303, 'ACT', 70000.00, '2024-08-18 17:00:00', 'Project Manager with a focus on agile methodologies.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (4, 'Bob', 'Brown', 'bob.brown@example.com', '2019-07-30', 40, 60606, 'INA', 75000.00, '2024-08-17 08:15:00', 'DevOps Engineer with a background in cloud infrastructure.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (5, 'Charlie', 'Davis', 'charlie.davis@example.com', '2018-03-22', 45, 94105, 'ACT', 80000.00, '2024-08-16 10:30:00', 'Database Administrator with expertise in PostgreSQL.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (6, 'Eva', 'Green', 'eva.green@example.com', '2017-09-05', 50, 33101, 'INA', 85000.00, '2024-08-15 11:45:00', 'Security Analyst with 10 years of experience.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (7, 'Frank', 'Harris', 'frank.harris@example.com', '2016-02-14', 38, 20001, 'ACT', 62000.00, '2024-08-14 09:00:00', 'Software Developer with a passion for open-source projects.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (8, 'Grace', 'Lee', 'grace.lee@example.com', '2015-06-28', 42, 75201, 'ACT', 73000.00, '2024-08-13 13:30:00', 'UX/UI Designer with a background in web development.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (9, 'Henry', 'Martinez', 'henry.martinez@example.com', '2014-10-11', 33, 98101, 'ACT', 67000.00, '2024-08-12 15:45:00', 'Network Engineer with expertise in Cisco systems.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (10, 'Ivy', 'Nelson', 'ivy.nelson@example.com', '2013-01-19', 29, 02101, 'ACT', 71000.00, '2024-08-11 07:30:00', 'Systems Analyst focusing on large-scale enterprise solutions.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (11, 'Jack', 'Owens', 'jack.owens@example.com', '2012-04-15', 37, 55401, 'INA', 77000.00, '2024-08-10 12:15:00', 'Software Architect with a focus on microservices.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (12, 'Kate', 'Parker', 'kate.parker@example.com', '2011-08-25', 44, 19103, 'ACT', 88000.00, '2024-08-09 14:00:00', 'Technical Writer with experience in API documentation.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (13, 'Liam', 'Quinn', 'liam.quinn@example.com', '2010-12-03', 41, 78701, 'ACT', 92000.00, '2024-08-08 16:30:00', 'IT Consultant with a focus on digital transformation.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (14, 'Mia', 'Robinson', 'mia.robinson@example.com', '2009-05-29', 48, 63101, 'INA', 98000.00, '2024-08-07 10:45:00', 'Business Analyst specializing in financial systems.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (15, 'Noah', 'Scott', 'noah.scott@example.com', '2008-07-20', 36, 85001, 'ACT', 65000.00, '2024-08-06 11:15:00', 'Cloud Engineer with experience in AWS and Azure.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (16, 'Olivia', 'Taylor', 'olivia.taylor@example.com', '2007-09-14', 39, 37201, 'ACT', 74000.00, '2024-08-05 08:45:00', 'Marketing Specialist with expertise in SEO.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (17, 'Paul', 'Upton', 'paul.upton@example.com', '2006-11-09', 35, 80202, 'ACT', 83000.00, '2024-08-04 13:00:00', 'Sales Manager with a focus on SaaS products.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (18, 'Quinn', 'Vance', 'quinn.vance@example.com', '2005-03-15', 43, 94102, 'ACT', 68000.00, '2024-08-03 14:30:00', 'Web Developer specializing in frontend technologies.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (19, 'Rachel', 'White', 'rachel.white@example.com', '2004-12-22', 32, 98109, 'INA', 72000.00, '2024-08-02 09:15:00', 'Graphic Designer with a background in branding.');
INSERT INTO pgctest.Emp (EID, FIRST_NAME, LAST_NAME, EMAIL, HIRE_DT, AGE, "Zip", STATUS, SALARY, LAST_LOGIN, BIO)
VALUES (20, 'Sam', 'Young', 'sam.young@example.com', '2003-01-17', 49, 60611, 'ACT', 91000.00, '2024-08-01 17:00:00', 'Product Manager with experience in e-commerce.');



-- Creating the test_common table in DB2
CREATE TABLE pgctest.test_common
(
    id INTEGER PRIMARY KEY,
    col_small INTEGER,
    col_int INTEGER,
    col_bigint BIGINT,
    col_dec_20 DECIMAL(20,0),
    col_dec_38 DECIMAL(38,0),
    col_dec_10_2 DECIMAL(10,2),
    col_float32 FLOAT,
    col_float64 DOUBLE,
    col_charnull VARCHAR(30),
    col_char_2 CHAR(2),
    col_string VARCHAR(4000),
    col_date DATE,
    col_datetime TIMESTAMP(3),
    col_tstz TIMESTAMP WITH TIME ZONE,
    col_ts6 TIMESTAMP(6),
    col_ts6tz TIMESTAMP(6) WITH TIME ZONE,
    col_dec_38_9 DECIMAL(38,9),
    col_dec_38_30 DECIMAL(38,30)
);

-- Inserting data into test_common
INSERT INTO pgctest.test_common (id, col_small, col_int, col_bigint, col_dec_20, col_dec_38, col_dec_10_2, col_float32, col_float64, col_charnull, col_char_2, col_string, col_date, col_datetime, col_tstz, col_ts6, col_ts6tz, col_dec_38_9, col_dec_38_30)
VALUES (1, 1, 1, 1, 12345678901234567890, 1234567890123456789012345, 123.11, 123456.1, 12345678.1, 'abc', 'A ', 'when in the course of human events it becomes necessary...', '1970-01-01', '1776-07-04 00:00:01', '1776-07-04 00:00:01-01:00', '1776-07-04 00:00:01.123456', '1776-07-04 00:00:01.123456-01:00', 12345678901234567890123456789.123456789, 12345678.123456789012345678901234567890);
INSERT INTO pgctest.test_common (id, col_small, col_int, col_bigint, col_dec_20, col_dec_38, col_dec_10_2, col_float32, col_float64, col_charnull, col_char_2, col_string, col_date, col_datetime, col_tstz, col_ts6, col_ts6tz, col_dec_38_9, col_dec_38_30)
VALUES (2, 2, 2, 2, 12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2,'',   'B' ,'when in the course of human events it becomes necessary...', '1970-01-02', '1991-01-02 00:00:02', '1991-01-02 00:00:02 -02:00', '1991-01-02 00:00:02.123456 -02:00',  '1991-01-02 00:00:02.123456 -02:00',22345678901234567890123456789.123456789,22345678.123456789012345678901234567890);
INSERT INTO pgctest.test_common (id, col_small, col_int, col_bigint, col_dec_20, col_dec_38, col_dec_10_2, col_float32, col_float64, col_charnull, col_char_2, col_string, col_date, col_datetime, col_tstz, col_ts6, col_ts6tz, col_dec_38_9, col_dec_38_30)
VALUES (3, 3, 3, 3, 12345678901234567890,1234567890123456789012345,123.3, 123456.3,12345678.3,'xyx','C ','when in the course of human events it becomes necessary...', '1970-01-03', '2030-01-03 00:00:03', '2030-01-03 00:00:03 -03:00', '2030-01-03 00:00:03.123456 -03:00',  '2030-01-03 00:00:03.123456 -03:00',32345678901234567890123456789.123456789,32345678.123456789012345678901234567890);
