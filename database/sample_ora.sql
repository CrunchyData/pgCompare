CREATE TABLE emp (EID int generated always as identity increment by 1 start with 1 not null,
                  first_name varchar2(40),
                  last_name varchar2(40),
                  email varchar2(100),
                  hire_dt timestamp,
                  last_update timestamp default systimestamp,
                  constraint emp_pk primary key (eid)
                  );

INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('John', 'Doe', 'johndoe@example.com', to_date('2021-01-15 09:00:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Jane', 'Smith', 'janesmith@example.com', to_date('2022-03-20 14:30:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Michael', 'Johnson', 'michaelj@example.com', to_date('2020-12-10 10:15:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Emily', 'Williams', 'emilyw@example.com', to_date('2023-05-05 08:45:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('David', 'Brown', 'davidbrown@example.com', to_date('2019-11-25 11:20:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Sarah', 'Taylor', 'saraht@example.com', to_date('2022-09-08 13:00:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Robert', 'Anderson', 'roberta@example.com', to_date('2021-07-12 16:10:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Jennifer', 'Martinez', 'jenniferm@example.com', to_date('2023-02-18 09:30:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('William', 'Jones', 'williamj@example.com', to_date('2020-04-30 12:45:00','YYYY-MM-DD HH24:MI:SS'));
INSERT INTO emp (FIRST_NAME,LAST_NAME,EMAIL,HIRE_DT) VALUES ('Linda', 'Garcia', 'lindag@example.com', to_date('2018-06-03 15:55:00','YYYY-MM-DD HH24:MI:SS'));
