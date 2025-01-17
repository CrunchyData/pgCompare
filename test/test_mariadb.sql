create database pgctest;

SET time_zone = '+00:00';

--
-- Test using Case sensitive table and column names
--
DROP TABLE IF EXISTS pgctest.Test_Case;
CREATE TABLE pgctest.Test_Case (
    eid int4,
    first_name varchar(40) NULL,
    last_name varchar(40) NULL,
    email varchar(100) NULL,
    hire_dt date NULL,
    age int4 NULL,
    Zip int4 NULL,
    status char(3) NULL,
    salary numeric(12, 2) NULL,
    last_login timestamp NULL,
    bio text NULL,
    CONSTRAINT Test_Case_pkey PRIMARY KEY (eid)
);

INSERT INTO pgctest.Test_Case (eid, first_name, last_name, email, hire_dt, age, Zip, status, salary, last_login, bio) VALUES
(1,'John', 'Doe', 'john.doe@example.com', '2022-01-15', 30, 90210, 'ACT', 60000.00, '2024-08-20 09:30:00', 'Software Engineer with 5 years of experience.'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2021-05-23', 28, 10001, 'ACT', 65000.00, '2024-08-19 14:45:00', 'Data Scientist specializing in machine learning.'),
(3, 'Alice', 'Johnson', 'alice.johnson@example.com', '2020-11-12', 34, 30303, 'ACT', 70000.00, '2024-08-18 17:00:00', 'Project Manager with a focus on agile methodologies.'),
(4, 'Bob', 'Brown', 'bob.brown@example.com', '2019-07-30', 40, 60606, 'INA', 75000.00, '2024-08-17 08:15:00', 'DevOps Engineer with a background in cloud infrastructure.'),
(5, 'Charlie', 'Davis', 'charlie.davis@example.com', '2018-03-22', 45, 94105, 'ACT', 80000.00, '2024-08-16 10:30:00', 'Database Administrator with expertise in PostgreSQL.'),
(6, 'Eva', 'Green', 'eva.green@example.com', '2017-09-05', 50, 33101, 'INA', 85000.00, '2024-08-15 11:45:00', 'Security Analyst with 10 years of experience.'),
(7, 'Frank', 'Harris', 'frank.harris@example.com', '2016-02-14', 38, 20001, 'ACT', 62000.00, '2024-08-14 09:00:00', 'Software Developer with a passion for open-source projects.'),
(8, 'Grace', 'Lee', 'grace.lee@example.com', '2015-06-28', 42, 75201, 'ACT', 73000.00, '2024-08-13 13:30:00', 'UX/UI Designer with a background in web development.'),
(9, 'Henry', 'Martinez', 'henry.martinez@example.com', '2014-10-11', 33, 98101, 'ACT', 67000.00, '2024-08-12 15:45:00', 'Network Engineer with expertise in Cisco systems.'),
(10, 'Ivy', 'Nelson', 'ivy.nelson@example.com', '2013-01-19', 29, 02101, 'ACT', 71000.00, '2024-08-11 07:30:00', 'Systems Analyst focusing on large-scale enterprise solutions.'),
(11, 'Jack', 'Owens', 'jack.owens@example.com', '2012-04-15', 37, 55401, 'INA', 77000.00, '2024-08-10 12:15:00', 'Software Architect with a focus on microservices.'),
(12, 'Kate', 'Parker', 'kate.parker@example.com', '2011-08-25', 44, 19103, 'ACT', 88000.00, '2024-08-09 14:00:00', 'Technical Writer with experience in API documentation.'),
(13, 'Liam', 'Quinn', 'liam.quinn@example.com', '2010-12-03', 41, 78701, 'ACT', 92000.00, '2024-08-08 16:30:00', 'IT Consultant with a focus on digital transformation.'),
(14, 'Mia', 'Robinson', 'mia.robinson@example.com', '2009-05-29', 48, 63101, 'INA', 98000.00, '2024-08-07 10:45:00', 'Business Analyst specializing in financial systems.'),
(15, 'Noah', 'Scott', 'noah.scott@example.com', '2008-07-20', 36, 85001, 'ACT', 65000.00, '2024-08-06 11:15:00', 'Cloud Engineer with experience in AWS and Azure.'),
(16, 'Olivia', 'Taylor', 'olivia.taylor@example.com', '2007-09-14', 39, 37201, 'ACT', 74000.00, '2024-08-05 08:45:00', 'Marketing Specialist with expertise in SEO.'),
(17, 'Paul', 'Upton', 'paul.upton@example.com', '2006-11-09', 35, 80202, 'ACT', 83000.00, '2024-08-04 13:00:00', 'Sales Manager with a focus on SaaS products.'),
(18, 'Quinn', 'Vance', 'quinn.vance@example.com', '2005-03-15', 43, 94102, 'ACT', 68000.00, '2024-08-03 14:30:00', 'Web Developer specializing in frontend technologies.'),
(19, 'Rachel', 'White', 'rachel.white@example.com', '2004-12-22', 32, 98109, 'INA', 72000.00, '2024-08-02 09:15:00', 'Graphic Designer with a background in branding.'),
(20, 'Sam', 'Young', 'sam.young@example.com', '2003-01-17', 49, 60611, 'ACT', 91000.00, '2024-08-01 17:00:00', 'Product Manager with experience in e-commerce.');

DROP TABLE IF EXISTS pgctest.test_nbr;
CREATE TABLE pgctest.test_nbr
(   id              int NOT NULL PRIMARY KEY
,   col_small       smallint
,   col_int         int
,   col_bigint      bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     real
,   col_float64     double precision
,   col_dec_38_9    DECIMAL(38,9)
,   col_dec_38_30   DECIMAL(38,30)
);

INSERT INTO pgctest.test_nbr VALUES
(1, 1, 1, 1, 12345678901234567890, 1234567890123456789012345, 123.11, 123456.1, 12345678.1, 12345678901234567890123456789.123456789,12345678.123456789012345678901234567890),
(2, 2, 2, 2, 12345678901234567890, 1234567890123456789012345, 123.22, 123456.2, 12345678.2, 22345678901234567890123456789.123456789,22345678.123456789012345678901234567890),
(3, 3, 3, 3, 12345678901234567890, 1234567890123456789012345, 123.3 , 123456.3, 12345678.3, 32345678901234567890123456789.123456789,32345678.123456789012345678901234567890),
(4, 4, 4, 4, null                , 1234567890123456789012345, 123.3 , 123456.3, 12345678.3, null                                   ,32345678.123456789012345678901234567890),
(5, -5, -5, -5, -12345678901234567890, -1234567890123456789012345, -123.3 , -123456.3, -12345678.3, -32345678901234567890123456789.123456789, -32345678.123456789012345678901234567890);

DROP TABLE IF EXISTS pgctest.test_char;
CREATE TABLE pgctest.test_char
(   id              int NOT NULL PRIMARY KEY
,   col_charnull    varchar(30)
,   col_char_2      char(2)
,   col_string      text
);

INSERT INTO pgctest.test_char VALUES
(1, 'abc','A ','when in the course of human events it becomes necessary...'),
(2, ''   ,'B' ,'when in the course of human events it becomes necessary...'),
(3, 'xyx','C ','when in the course of human events it becomes necessary...'),
(4, null, 'D ','when in the course of human events it becomes necessary...');

DROP TABLE IF EXISTS pgctest.test_dt;
CREATE TABLE pgctest.test_dt
(   id              int NOT NULL PRIMARY KEY
,   col_date        date
,   col_datetime    timestamp(3)
,   col_tstz        timestamp(3)
,   col_ts6         timestamp(6)
,   col_ts6tz       timestamp(6)
);

INSERT INTO pgctest.test_dt VALUES
(1, '1970-01-01', '1976-07-04 01:00:01', convert_tz('1976-07-04 02:00:01','-01:00','UTC'), '1976-07-04 03:00:01.123456',  convert_tz('1976-07-04 04:00:01.123456','-01:00','UTC')),
(2, '1970-01-02', '1991-01-02 01:00:02', convert_tz('1991-01-02 02:00:02','-02:00','UTC'), '1991-01-02 03:00:02.123456',  convert_tz('1991-01-02 04:00:02.123456','-02:00','UTC')),
(3, '1970-01-03', '2030-01-03 01:00:03', convert_tz('2030-01-03 02:00:03','-03:00','UTC'), '2030-01-03 03:00:03.123456',  convert_tz('2030-01-03 04:00:03.123456','-03:00','UTC')),
(4, '1970-01-04', '2030-01-04 01:00:03', convert_tz('2030-01-04 02:00:03','-03:00','UTC'), null                                        ,  null                    );

DROP TABLE IF EXISTS pgctest.multipk;
CREATE TABLE pgctest.multipk (
	COL_1 varchar(10),
	PK int NOT NULL,
	pk2 int NOT NULL,
	CONSTRAINT multipk_pk PRIMARY KEY (PK, pk2)
);

INSERT INTO pgctest.multipk (PK, pk2, COL_1) VALUES (1, 1, 'test');

DROP TABLE IF EXISTS pgctest.plat;
CREATE TABLE pgctest.plat (
   id int NOT NULL,
   plat varchar(10),
   CONSTRAINT plat_pk PRIMARY KEY (id));

INSERT INTO pgctest.plat (id, plat) VALUES (1, 'mariadb');

DROP TABLE IF EXISTS pgctest.test_enum;
CREATE TABLE pgctest.test_enum (
  id int(11) NOT NULL,
  status enum('abc','def','ghi'),
  CONSTRAINT test_enum_pk PRIMARY KEY (id)
);

INSERT INTO pgctest.test_enum (id, status) values (1, 'abc');