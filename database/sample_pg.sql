create schema pgctest;

CREATE TABLE pgctest.emp (eid int generated always as identity (start with 1 increment by 1) primary key,
                  first_name varchar(40),
                  last_name varchar(40),
                  email varchar(100),
                  hire_dt date,
                  age int,
                  zip int,
                  status char(3),
                  salary numeric(12,2),
                  last_login timestamp,
                  bio text
                  );

INSERT INTO pgctest.emp (first_name, last_name, email, hire_dt, age, zip, status, salary, last_login, bio) VALUES
('Mickey', 'Mouse', 'mickey.mouse@disney.com', TO_DATE('2010-05-12', 'YYYY-MM-DD'), 35, 90210, 'ACT', 60000.00, TO_TIMESTAMP('2024-06-15 08:30:00', 'YYYY-MM-DD HH24:MI:SS'), 'Mickey is the beloved cartoon character created by Walt Disney.'),
('Donald', 'Duck', 'donald.duck@disney.com', TO_DATE('2012-03-22', 'YYYY-MM-DD'), 33, 90211, 'ACT', 55000.00, TO_TIMESTAMP('2024-06-16 09:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Donald is a classic Disney character known for his temper and distinctive voice.'),
('Bugs', 'Bunny', 'bugs.bunny@wb.com', TO_DATE('2011-07-19', 'YYYY-MM-DD'), 32, 90001, 'ACT', 58000.00, TO_TIMESTAMP('2024-06-17 11:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Bugs is the iconic character from Looney Tunes, famous for his catchphrase "What\'s up, Doc?".'),
('Scooby', 'Doo', 'scooby.doo@hanna-barbera.com', TO_DATE('2009-11-25', 'YYYY-MM-DD'), 40, 90212, 'ACT', 62000.00, TO_TIMESTAMP('2024-06-18 14:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Scooby-Doo is the lovable Great Dane from the Hanna-Barbera animated series.'),
('Fred', 'Flintstone', 'fred.flintstone@hanna-barbera.com', TO_DATE('2006-08-30', 'YYYY-MM-DD'), 50, 90214, 'ACT', 48000.00, TO_TIMESTAMP('2024-06-19 18:30:00', 'YYYY-MM-DD HH24:MI:SS'), 'Fred is the prehistoric family man from The Flintstones.'),
('George', 'Jetson', 'george.jetson@hanna-barbera.com', TO_DATE('2013-02-14', 'YYYY-MM-DD'), 38, 90215, 'ACT', 52000.00, TO_TIMESTAMP('2024-06-19 19:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'George is the father in the futuristic family from The Jetsons.'),
('Tom', 'Cat', 'tom.cat@mgm.com', TO_DATE('2016-12-10', 'YYYY-MM-DD'), 30, 90004, 'ACT', 54000.00, TO_TIMESTAMP('2024-06-19 20:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Tom is the cat always chasing Jerry in the classic MGM animated series.');

CREATE TABLE pgctest.location (lid int generated always as identity (start with 1 increment by 1) primary key,
                          longitude numeric(10,8),
                          latitude numeric(10,8),
                          city varchar(100),
                          state char(2),
                          zip int,
                          location_code numeric
                          );

INSERT INTO pgctest.location (longitude, latitude, city, state, zip, location_code) VALUES
(-73.935242, 40.730610, 'New York', 'NY', 10001, 1234567890.123456789),
(-118.243683, 34.052235, 'Los Angeles', 'CA', 90001, 1234567890.123456789),
(-87.623177, 41.881832, 'Chicago', 'IL', 60601, 1234567890.123456789),
(-95.358421, 29.749907, 'Houston', 'TX', 77001, 1234567890.123456789),
(-75.165222, 39.952583, 'Philadelphia', 'PA', 19101, 1234567890.123456789),
(-112.074036, 33.448376, 'Phoenix', 'AZ', 85001, 1234567890.123456789),
(-122.419418, 37.774929, 'San Francisco', 'CA', 94101, 1234567890.123456789),
(-84.388229, 33.749001, 'Atlanta', 'GA', 30301, 108),
(-80.191788, 25.761681, 'Miami', 'FL', 33101, 1234567890.123456789),
(-71.058884, 42.360081, 'Boston', 'MA', 02101, 1234567890.123456789);

commit;