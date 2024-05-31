CREATE TABLE emp (EID int generated always as identity increment by 1 start with 1 not null,
                  first_name varchar2(40),
                  last_name varchar2(40),
                  email varchar2(100),
                  hire_dt timestamp,
                  last_update timestamp default systimestamp,
                  constraint emp_pk primary key (eid)
                  );

INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Mickey', 'Mouse', 'mickey.mouse@disney.com', '1928-11-18 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Minnie', 'Mouse', 'minnie.mouse@disney.com', '1928-11-18 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Donald', 'Duck', 'donald.duck@disney.com', '1934-06-09 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Daisy', 'Duck', 'daisy.duck@disney.com', '1940-06-07 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Goofy', 'Goof', 'goofy.goof@disney.com', '1932-05-25 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Pluto', 'Dog', 'pluto.dog@disney.com', '1930-09-05 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Huey', 'Duck', 'huey.duck@disney.com', '1937-10-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Dewey', 'Duck', 'dewey.duck@disney.com', '1937-10-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Louie', 'Duck', 'louie.duck@disney.com', '1937-10-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Scrooge', 'McDuck', 'scrooge.mcduck@disney.com', '1947-12-22 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Bugs', 'Bunny', 'bugs.bunny@looneytunes.com', '1940-07-27 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Daffy', 'Duck', 'daffy.duck@looneytunes.com', '1937-04-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Porky', 'Pig', 'porky.pig@looneytunes.com', '1935-03-02 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Elmer', 'Fudd', 'elmer.fudd@looneytunes.com', '1940-03-02 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Sylvester', 'Cat', 'sylvester.cat@looneytunes.com', '1945-03-24 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Tweety', 'Bird', 'tweety.bird@looneytunes.com', '1942-11-21 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Taz', 'Devil', 'taz.devil@looneytunes.com', '1954-06-19 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Marvin', 'Martian', 'marvin.martian@looneytunes.com', '1948-07-24 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Yosemite', 'Sam', 'yosemite.sam@looneytunes.com', '1945-05-05 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Foghorn', 'Leghorn', 'foghorn.leghorn@looneytunes.com', '1946-08-31 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Speedy', 'Gonzales', 'speedy.gonzales@looneytunes.com', '1953-09-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Wile E.', 'Coyote', 'wile.e.coyote@looneytunes.com', '1949-09-17 00:00:00');
INSERT INTO emp (first_name, last_name, email, hire_dt) VALUES ('Road', 'Runner', 'road.runner@looneytunes.com', '1949-09-17 00:00:00');