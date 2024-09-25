-- Assuming schema 'pgctest' is already created or implicitly created

CREATE TABLE pgctest.emp (
    EID INT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL,
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    email VARCHAR(100),
    hire_dt DATE,
    age SMALLINT,  -- DB2 uses SMALLINT for smaller numbers like age
    zip INTEGER,
    status CHAR(3),
    salary DECIMAL(12,2),  -- Equivalent to Oracle's NUMBER(12,2)
    last_login TIMESTAMP,
    bio VARCHAR(2000),
    CONSTRAINT emp_pk PRIMARY KEY (EID)
);

-- Insert records into the EMP table
INSERT INTO pgctest.emp (first_name, last_name, email, hire_dt, age, zip, status, salary, last_login, bio) VALUES
('Mickey', 'Mouse', 'mickey.mouse@disney.com', '2010-05-12', 35, 90210, 'ACT', 60000.00, '2024-06-15 08:30:00', 'Mickey is the beloved cartoon character created by Walt Disney.'),
('Donald', 'Duck', 'donald.duck@disney.com', '2012-03-22', 33, 90211, 'ACT', 55000.00, '2024-06-16 09:00:00', 'Donald is a classic Disney character known for his temper and distinctive voice.'),
('Bugs', 'Bunny', 'bugs.bunny@wb.com', '2011-07-19', 32, 90001, 'ACT', 58000.00, '2024-06-17 11:00:00', 'Bugs is the iconic character from Looney Tunes, famous for his catchphrase "What\'s up, Doc?".'),
('Scooby', 'Doo', 'scooby.doo@hanna-barbera.com', '2009-11-25', 40, 90212, 'ACT', 62000.00, '2024-06-18 14:00:00', 'Scooby-Doo is the lovable Great Dane from the Hanna-Barbera animated series.'),
('Fred', 'Flintstone', 'fred.flintstone@hanna-barbera.com', '2006-08-30', 50, 90214, 'ACT', 48000.00, '2024-06-19 18:30:00', 'Fred is the prehistoric family man from The Flintstones.'),
('George', 'Jetson', 'george.jetson@hanna-barbera.com', '2013-02-14', 38, 90215, 'ACT', 52000.00, '2024-06-19 19:00:00', 'George is the father in the futuristic family from The Jetsons.'),
('Tom', 'Cat', 'tom.cat@mgm.com', '2016-12-10', 30, 90004, 'ACT', 54000.00, '2024-06-19 20:00:00', 'Tom is the cat always chasing Jerry in the classic MGM animated series.');

-- Create the LOCATION table
CREATE TABLE pgctest.location (
    lid INT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL,
    longitude DECIMAL(10,8),  -- DB2's equivalent to Oracle's NUMBER(10,8)
    latitude DECIMAL(10,8),
    city VARCHAR(100),
    state CHAR(2),
    zip INTEGER,
    location_code DECIMAL(20,8)  -- Using DECIMAL(20,8) for large numbers with decimals
);

-- Insert records into the LOCATION table
INSERT INTO pgctest.location (longitude, latitude, city, state, zip, location_code) VALUES
(-73.935242, 40.730610, 'New York', 'NY', 10001, 1234567890.12345678),
(-118.243683, 34.052235, 'Los Angeles', 'CA', 90001, 1234567890.12345678),
(-87.623177, 41.881832, 'Chicago', 'IL', 60601, 1234567890.12345678),
(-95.358421, 29.749907, 'Houston', 'TX', 77001, 1234567890.12345678),
(-75.165222, 39.952583, 'Philadelphia', 'PA', 19101, 1234567890.12345678),
(-112.074036, 33.448376, 'Phoenix', 'AZ', 85001, 1234567890.12345678),
(-122.419418, 37.774929, 'San Francisco', 'CA', 94101, 1234567890.12345678),
(-84.388229, 33.749001, 'Atlanta', 'GA', 30301, 108),
(-80.191788, 25.761681, 'Miami', 'FL', 33101, 1234567890.12345678),
(-71.058884, 42.360081, 'Boston', 'MA', 2101, 1234567890.12345678);

-- Commit the changes
COMMIT;
