create user pgctest identified by "welcome1";
grant unlimited tablespace to pgctest;
grant connect,resource to pgctest;

CREATE TABLE pgctest."Test_Case"
   (EID NUMBER,
    FIRST_NAME VARCHAR2(40 BYTE),
    LAST_NAME VARCHAR2(40 BYTE),
    EMAIL VARCHAR2(100 BYTE),
    HIRE_DT DATE,
    AGE NUMBER(3,0),
    "Zip" NUMBER(5,0),
    STATUS CHAR(3 BYTE),
    SALARY NUMBER(12,2),
    LAST_LOGIN TIMESTAMP (6),
    BIO VARCHAR2(2000 BYTE)
   );

ALTER TABLE pgctest."Test_Case" ADD CONSTRAINT "Test_Case_pkey" PRIMARY KEY ("EID");

Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (1,'John','Doe','john.doe@example.com',to_date('15-JAN-22','DD-MON-RR'),30,90210,'ACT',60000,to_timestamp('20-AUG-24 09.30.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Software Engineer with 5 years of experience.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (2,'Jane','Smith','jane.smith@example.com',to_date('23-MAY-21','DD-MON-RR'),28,10001,'ACT',65000,to_timestamp('19-AUG-24 02.45.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Data Scientist specializing in machine learning.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (3,'Alice','Johnson','alice.johnson@example.com',to_date('12-NOV-20','DD-MON-RR'),34,30303,'ACT',70000,to_timestamp('18-AUG-24 05.00.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Project Manager with a focus on agile methodologies.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (4,'Bob','Brown','bob.brown@example.com',to_date('30-JUL-19','DD-MON-RR'),40,60606,'INA',75000,to_timestamp('17-AUG-24 08.15.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'DevOps Engineer with a background in cloud infrastructure.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (5, 'Charlie', 'Davis', 'charlie.davis@example.com', to_date('22-MAR-18','DD-MON-RR'), 45, 94105, 'ACT', 80000.00, to_timestamp('16-AUG-24 10:30:00','DD-MON-RR HH.MI.SSXFF AM'), 'Database Administrator with expertise in PostgreSQL.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (6,'Eva','Green','eva.green@example.com',to_date('05-SEP-17','DD-MON-RR'),50,33101,'INA',85000,to_timestamp('15-AUG-24 11.45.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Security Analyst with 10 years of experience.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (7,'Frank','Harris','frank.harris@example.com',to_date('14-FEB-16','DD-MON-RR'),38,20001,'ACT',62000,to_timestamp('14-AUG-24 09.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Software Developer with a passion for open-source projects.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (8,'Grace','Lee','grace.lee@example.com',to_date('28-JUN-15','DD-MON-RR'),42,75201,'ACT',73000,to_timestamp('13-AUG-24 01.30.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'UX/UI Designer with a background in web development.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (9,'Henry','Martinez','henry.martinez@example.com',to_date('11-OCT-14','DD-MON-RR'),33,98101,'ACT',67000,to_timestamp('12-AUG-24 03.45.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Network Engineer with expertise in Cisco systems.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (10,'Ivy','Nelson','ivy.nelson@example.com',to_date('19-JAN-13','DD-MON-RR'),29,2101,'ACT',71000,to_timestamp('11-AUG-24 07.30.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Systems Analyst focusing on large-scale enterprise solutions.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (11,'Jack','Owens','jack.owens@example.com',to_date('15-APR-12','DD-MON-RR'),37,55401,'INA',77000,to_timestamp('10-AUG-24 12.15.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Software Architect with a focus on microservices.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (12,'Kate','Parker','kate.parker@example.com',to_date('25-AUG-11','DD-MON-RR'),44,19103,'ACT',88000,to_timestamp('09-AUG-24 02.00.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Technical Writer with experience in API documentation.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (13,'Liam','Quinn','liam.quinn@example.com',to_date('03-DEC-10','DD-MON-RR'),41,78701,'ACT',92000,to_timestamp('08-AUG-24 04.30.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'IT Consultant with a focus on digital transformation.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (14,'Mia','Robinson','mia.robinson@example.com',to_date('29-MAY-09','DD-MON-RR'),48,63101,'INA',98000,to_timestamp('07-AUG-24 10.45.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Business Analyst specializing in financial systems.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (15,'Noah','Scott','noah.scott@example.com',to_date('20-JUL-08','DD-MON-RR'),36,85001,'ACT',65000,to_timestamp('06-AUG-24 11.15.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Cloud Engineer with experience in AWS and Azure.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (16,'Olivia','Taylor','olivia.taylor@example.com',to_date('14-SEP-07','DD-MON-RR'),39,37201,'ACT',74000,to_timestamp('05-AUG-24 08.45.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Marketing Specialist with expertise in SEO.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (17,'Paul','Upton','paul.upton@example.com',to_date('09-NOV-06','DD-MON-RR'),35,80202,'ACT',83000,to_timestamp('04-AUG-24 01.00.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Sales Manager with a focus on SaaS products.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (18,'Quinn','Vance','quinn.vance@example.com',to_date('15-MAR-05','DD-MON-RR'),43,94102,'ACT',68000,to_timestamp('03-AUG-24 02.30.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Web Developer specializing in frontend technologies.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (19,'Rachel','White','rachel.white@example.com',to_date('22-DEC-04','DD-MON-RR'),32,98109,'INA',72000,to_timestamp('02-AUG-24 09.15.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Graphic Designer with a background in branding.');
Insert into pgctest."Test_Case" (EID,FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT,AGE,"Zip",STATUS,SALARY,LAST_LOGIN,BIO) values (20,'Sam','Young','sam.young@example.com',to_date('17-JAN-03','DD-MON-RR'),49,60611,'ACT',91000,to_timestamp('01-AUG-24 05.00.00.000000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'Product Manager with experience in e-commerce.');

DROP TABLE IF EXISTS pgctest.test_nbr;
CREATE TABLE pgctest.test_nbr
(   id              NUMBER(8) NOT NULL PRIMARY KEY
,   col_small       NUMBER(4)
,   col_int         NUMBER(9)
,   col_bigint      NUMBER(18)
,   col_dec_20      NUMBER(20)
,   col_dec_38      NUMBER(38)
,   col_dec_10_2    NUMBER(10,2)
,   col_float32     binary_float
,   col_float64     binary_double
,   col_dec_38_9    NUMBER(38,9)
,   col_dec_38_30   NUMBER(38,30)
);

INSERT INTO pgctest.test_nbr VALUES (1, 1, 1, 1, 12345678901234567890, 1234567890123456789012345, 123.11, 123456.1, 12345678.1, 12345678901234567890123456789.123456789, 12345678.123456789012345678901234567890);
INSERT INTO pgctest.test_nbr VALUES (2, 2, 2, 2, 12345678901234567890, 1234567890123456789012345, 123.22, 123456.2, 12345678.2, 22345678901234567890123456789.123456789, 22345678.123456789012345678901234567890);
INSERT INTO pgctest.test_nbr VALUES (3, 3, 3, 3, 12345678901234567890, 1234567890123456789012345, 123.3 , 123456.3, 12345678.3, 32345678901234567890123456789.123456789, 32345678.123456789012345678901234567890);
INSERT INTO pgctest.test_nbr VALUES (4, 4, 4, 4, null                , 1234567890123456789012345, 123.3 , 123456.3, 12345678.3, null                                   , 32345678.123456789012345678901234567890);
INSERT INTO pgctest.test_nbr VALUES (5, -5, -5, -5, -12345678901234567890, -1234567890123456789012345, -123.3 , -123456.3, -12345678.3, -32345678901234567890123456789.123456789, -32345678.123456789012345678901234567890);

DROP TABLE IF EXISTS pgctest.test_char;
CREATE TABLE pgctest.test_char
(   id              NUMBER(8) NOT NULL PRIMARY KEY
,   col_charnull    varchar2(30)
,   col_char_2      char(2)
,   col_string      varchar2(4000)
);

INSERT INTO pgctest.test_char VALUES (1, 'abc', 'A ', 'when in the course of human events it becomes necessary...');
INSERT INTO pgctest.test_char VALUES (2, ''   , 'B' , 'when in the course of human events it becomes necessary...');
INSERT INTO pgctest.test_char VALUES (3, 'xyx', 'C ', 'when in the course of human events it becomes necessary...');
INSERT INTO pgctest.test_char VALUES (4, null , 'D ', 'when in the course of human events it becomes necessary...');


DROP TABLE IF EXISTS pgctest.test_dt;
CREATE TABLE pgctest.test_dt
(   id              NUMBER(8) NOT NULL PRIMARY KEY
,   col_date        date
,   col_datetime    timestamp(3)
,   col_tstz        timestamp(3) with time zone
,   col_ts6         timestamp(6)
,   col_ts6tz       timestamp(6) with time zone
);

INSERT INTO pgctest.test_dt VALUES (1, DATE'1970-01-01', TIMESTAMP'1976-07-04 01:00:01', to_timestamp_tz('1976-07-04 02:00:01 -01:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), TIMESTAMP'1976-07-04 03:00:01.123456',  to_timestamp_tz('1976-07-04 04:00:01.123456 -01:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'));
INSERT INTO pgctest.test_dt VALUES (2, DATE'1970-01-02', TIMESTAMP'1991-01-02 01:00:02', to_timestamp_tz('1991-01-02 02:00:02 -02:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), TIMESTAMP'1991-01-02 03:00:02.123456',  to_timestamp_tz('1991-01-02 04:00:02.123456 -02:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'));
INSERT INTO pgctest.test_dt VALUES (3, DATE'1970-01-03', TIMESTAMP'2030-01-03 01:00:03', to_timestamp_tz('2030-01-03 02:00:03 -03:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), TIMESTAMP'2030-01-03 03:00:03.123456',  to_timestamp_tz('2030-01-03 04:00:03.123456 -03:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'));
INSERT INTO pgctest.test_dt VALUES (4, DATE'1970-01-04', TIMESTAMP'2030-01-04 01:00:03', to_timestamp_tz('2030-01-04 02:00:03 -03:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), null                                 ,  null                                                                                    );

CREATE TABLE pgctest.multipk (
	"COL_1" varchar2(10) NULL,
	"PK" number(8) NOT NULL,
	pk2 number(8) NOT NULL,
	CONSTRAINT multipk_pk PRIMARY KEY ("PK", pk2)
);


INSERT INTO pgctest.multipk ("PK", pk2, "COL_1") VALUES (1, 1, 'test');

CREATE TABLE pgctest.plat (
   id number(8) NOT NULL,
   plat varchar2(10),
   CONSTRAINT plat_pk PRIMARY KEY (id));

INSERT INTO pgctest.plat (id, plat) VALUES (1, 'oracle');