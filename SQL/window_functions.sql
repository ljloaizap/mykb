-- ########################################################################
-- #########################  WINDOW FUNCTIONS  ###########################
-- ########################################################################

-- ------------------------------------------------------------------------
-- *****  RANK  *****
-- ------------------------------------------------------------------------

DROP TABLE IF EXISTS employee;
CREATE TABLE employee(
    employee_id int,
    full_name varchar(60),
    department varchar(30),
    salary numeric(10, 2)
);

INSERT INTO employee VALUES(100, 'Mary Johns', 'SALES', 1000);
INSERT INTO employee VALUES(101, 'Sean Moldy', 'IT', 1500);
INSERT INTO employee VALUES(102, 'Peter Dugan', 'SALES', 2000);
INSERT INTO employee VALUES(103, 'Lilian Penn', 'SALES', 1700);
INSERT INTO employee VALUES(104, 'Milton Kowarsky', 'IT', 1800);
INSERT INTO employee VALUES(105, 'Mareen Bisset', 'ACCOUNTS', 1200);
INSERT INTO employee VALUES(106, 'Airton Graue', 'ACCOUNTS', 1100);
INSERT INTO employee VALUES(201, 'Patricia Fernandz', 'ACCOUNTS', 1100);
INSERT INTO employee VALUES(202, 'La pupuchurra', 'ACCOUNTS', 1000);


SELECT
    row_number() OVER (PARTITION BY department ORDER BY salary DESC ) as rn,
    rank() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    department,
    employee_id AS e_id,
    full_name,
    salary
FROM employee;

SELECT
    department,
    salary,
    full_name,
    ROUND(salary / max(salary) OVER (PARTITION BY department), 2) AS metric
FROM employee
ORDER BY metric;


-- ------------------------------------------------------------------------
-- *****  LEAD  *****
-- ------------------------------------------------------------------------
DROP TABLE IF EXISTS train_schedule;
CREATE TABLE train_schedule(
    train_id int,
    station varchar(60),
    time_at time
);

INSERT INTO train_schedule VALUES(110, 'San Francisco', '10:00:00'::time);
INSERT INTO train_schedule VALUES(110, 'Redwood City', '10:54:00'::time);
INSERT INTO train_schedule VALUES(110, 'Palo Alto', '11:02:00'::time);
INSERT INTO train_schedule VALUES(110, 'San Jose', '12:35:00'::time);
INSERT INTO train_schedule VALUES(120, 'San Francisco', '11:00:00'::time);
INSERT INTO train_schedule VALUES(120, 'Redwood City', NULL);
INSERT INTO train_schedule VALUES(120, 'Palo Alto', '12:49:00'::time);
INSERT INTO train_schedule VALUES(120, 'San Jose', '13:30:00'::time);

-- "Lead" with train_schedule table
SELECT
    train_id,
    station,
    time_at AS st_time,
	(time_at - min(time_at) OVER (PARTITION BY train_id ORDER BY time_at))::time
								AS elapsed_travel_time,
    (lead(time_at) OVER(PARTITION BY train_id ORDER BY time_at) - time_at)::time AS _time_next
FROM train_schedule
WHERE time_at IS NOT NULL
ORDER BY 1, 3
;


-- "Lead" with employee table
SELECT
    employee_id AS e_id,
    full_name,
    department AS dept,
    salary,
    lead(salary, 2, -1) OVER (ORDER BY salary) AS next_next_salary
FROM employee;


-- More cases...
DROP TABLE IF EXISTS races;
CREATE TABLE races (
  pilot_name varchar(100),
  circuit_name varchar(30),
  year int,
  time_at time,
  finish bool
);

INSERT INTO races VALUES('Alonso', 'Monza', 2016, '1:57:06.32'::time, true);
INSERT INTO races VALUES('Hamilton', 'Monza', 2016, '1:51:54.28'::time, true);
INSERT INTO races VALUES('Vetel', 'Monza', 2016, '1:52:04.12'::time, true);
INSERT INTO races VALUES('Alonso', 'Montecarlo', 2016, '0:43:14.73'::time, false);
INSERT INTO races VALUES('Hamilton', 'Montecarlo', 2016, '1:12:09.12'::time, true);
INSERT INTO races VALUES('Vetel', 'Montecarlo', 2016, '0:21:54.73'::time, false);
INSERT INTO races VALUES('Raikonen', 'Montecarlo', 2016, '1:14:04.12'::time, true);
INSERT INTO races VALUES('Hamilton', 'Monza', 2017, '1:13:16.97'::time, true);
INSERT INTO races VALUES('Vetel', 'Monza', 2017, '1:11:39.12'::time, true);
INSERT INTO races VALUES('Raikonen', 'Montecarlo', 2017, '0:43:14.73'::time, false);
INSERT INTO races VALUES('Alonso', 'Montecarlo', 2017, '1:32:14.42'::time, true);
INSERT INTO races VALUES('Hamilton', 'Montecarlo', 2017, '0:43:14.73'::time, false);
INSERT INTO races VALUES('Vetel', 'Montecarlo', 2017, '1:33:04.12'::time, true);


-- Main average WF
SELECT
    pilot_name,
    circuit_name,
    year,
    time_at,
    AVG(time_at) OVER (PARTITION BY circuit_name)::time AS avg_circuit,
    AVG(time_at) OVER (PARTITION BY circuit_name, year)::time AS avg_race
FROM races
WHERE finish
ORDER BY year DESC, circuit_name, time_at;


-- ------------------------------------------------------------------------
-- *****  FIRST_VALUE, LAST_VALUE, NTH_VALUE  *****
-- ------------------------------------------------------------------------

-- Getting cool... query to obtain every pilot name, their time, 
-- their position in the race, the time of the race winner
-- and the delta time between this pilot and the winner.
SELECT
    pilot_name,
    circuit_name,
    year,
    time_at AS pilot_time,
    rank() OVER (PARTITION BY year, circuit_name ORDER BY time_at) AS pos_race,
    first_value(time_at) OVER (PARTITION BY year, circuit_name ORDER BY time_at) AS race_winner_time,
    (time_at - first_value(time_at) OVER (PARTITION BY year, circuit_name ORDER BY time_at))::time AS delta
FROM races
WHERE finish
ORDER BY year desc, circuit_name, time_at;

-- Another one...
SELECT
	pilot_name,
	circuit_name,
	year,
	time_at AS pilot_time,
	RANK() OVER (PARTITION BY circuit_name, year ORDER BY time_at) AS position,
	FIRST_VALUE(pilot_name) OVER (PARTITION BY circuit_name, year ORDER BY time_at) AS winner_pilot,
	LAST_VALUE(pilot_name)
	    OVER (PARTITION BY circuit_name, year
	          ORDER BY time_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_pilot
FROM races
WHERE finish
ORDER BY year desc, circuit_name, time_at;
