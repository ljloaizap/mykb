-- ########################################################################
-- #########################  WINDOW FUNCTIONS  ###########################
-- ########################################################################

-- ------------------------------------------------------------------------
-- *****  RANK & DENSE_RANK  *****
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
INSERT INTO employee VALUES(203, 'Almando', 'ACCOUNTS', 1100);


-- rank & dense_rank
SELECT
    row_number() OVER (PARTITION BY department ORDER BY salary DESC ) as rn,
    rank() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    dense_rank() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank,
    department,
    employee_id AS e_id,
    full_name,
    salary
FROM employee;

-- max as window function
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

-- Using sub-clause Order By / Rows between
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


-- ------------------------------------------------------------------------
-- *****  Exam with chatcito  *****
-- ------------------------------------------------------------------------

-- 1) Find the cumulative sum of order_amount for each customer_id over time,
-- ordered by order_date.
CREATE TABLE sales (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10, 2)
);

INSERT INTO sales (order_id, customer_id, order_date, order_amount)
VALUES
    (1, 101, '2023-01-01', 100.50),
    (2, 102, '2023-01-02', 75.25),
    (3, 101, '2023-01-03', 50.75),
    (4, 103, '2023-01-04', 200.00),
    (5, 102, '2023-01-05', 125.00),
    (6, 101, '2023-01-06', 75.50),
    (7, 103, '2023-01-07', 150.25);

-- Query_e1
SELECT
    customer_id,
    order_date,
    order_amount,
    sum(order_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS cumulative
FROM sales
ORDER BY
    customer_id,
    order_date;
    

-- 2) Find the highest salary in each department and the employee(s) who earn that salary.
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    department_id INT,
    salary DECIMAL(10, 2),
    hire_date DATE
);

INSERT INTO employees (employee_id, department_id, salary, hire_date)
VALUES
    (1, 101, 55000.00, '2022-01-01'),
    (2, 102, 60000.00, '2021-03-15'),
    (3, 101, 60000.00, '2022-02-15'),
    (4, 103, 75000.00, '2021-05-20'),
    (5, 102, 65000.00, '2021-08-10'),
    (6, 103, 80000.00, '2022-04-05'),
    (7, 101, 58000.00, '2023-01-10'),
    (8, 101, 60000.00, '2022-04-20');

-- Query_e2
SELECT
    department_id, employee_id, salary AS highest_salary
FROM (
    SELECT
        department_id,
        employee_id,
        salary,
        rank() OVER (PARTITION BY department_id ORDER BY salary DESC) = 1 AS has_highest
    FROM employees
) AS tbl
WHERE has_highest;


-- 3) Find the customer(s) with the highest total orders.
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_total DECIMAL(10, 2)
);

INSERT INTO orders (order_id, customer_id, order_date, order_total)
VALUES
    (1, 101, '2023-01-01', 500.00),
    (2, 102, '2023-01-02', 750.00),
    (3, 101, '2023-01-03', 600.00),
    (4, 103, '2023-01-04', 800.00),
    (5, 102, '2023-01-05', 900.00),
    (6, 101, '2023-01-06', 700.00),
    (7, 103, '2023-01-07', 950.00),
    (8, 102, '2023-01-08', 150.00);

-- Query_e3
SELECT
    customer_id, sum_order_total
FROM (
    SELECT
        customer_id,
        sum(order_total) AS sum_order_total,
        dense_rank() OVER (ORDER BY sum(order_total) DESC) AS rank
    FROM orders
    GROUP BY customer_id
) AS tbl
WHERE rank = 1;


-- 4) Each student can take the same course multiple times. Find the latest grade
-- for each student in each course.
DROP TABLE IF EXISTS students;
CREATE TABLE students (
    student_id INT,
    course_id INT,
    grade CHAR(1),
    exam_date DATE
);

INSERT INTO students (student_id, course_id, grade, exam_date)
VALUES
    (1, 101, 'A', '2023-01-15'),
    (1, 101, 'B', '2023-02-10'),
    (2, 102, 'C', '2023-03-05'),
    (2, 102, 'B', '2023-04-20'),
    (3, 101, 'A', '2023-02-05'),
    (3, 101, 'A', '2023-03-15'),
    (3, 102, 'B', '2023-04-10');

-- Query_e4
SELECT
    student_id, course_id, grade AS latest_grade
FROM (
    SELECT
        student_id,
        course_id,
        grade,
        dense_rank() OVER (PARTITION BY student_id, course_id ORDER BY exam_date DESC) AS rank
    FROM students
    ORDER BY student_id, course_id, exam_date DESC
) AS tbl
WHERE rank = 1;


-- 5) Find the top-selling product for each month of the year,
-- along with the total quantity sold and revenue for that product in each month.
DROP TABLE IF EXISTS sales_transactions;
CREATE TABLE sales_transactions (
    transaction_id INT PRIMARY KEY,
    product_id INT,
    transaction_date DATE,
    quantity_sold INT,
    revenue DECIMAL(10, 2)
);

INSERT INTO sales_transactions (transaction_id, product_id, transaction_date, quantity_sold, revenue)
VALUES
    (1, 101, '2023-01-15', 50, 1000.00),
    (2, 102, '2023-01-20', 30, 750.00),
    (3, 101, '2023-02-10', 45, 900.00),
    (4, 102, '2023-02-15', 40, 800.00),
    (5, 103, '2023-03-05', 60, 1200.00),
    (6, 101, '2023-03-15', 55, 1100.00),
    (7, 103, '2023-03-20', 65, 1300.00),
    (8, 102, '2023-04-10', 35, 700.00),
    (9, 101, '2023-04-20', 25, 500.00);

-- Query_e5
SELECT DISTINCT
    to_char(transaction_date, 'yyyy-MM') AS month,
    first_value(product_id) OVER (
        PARTITION BY to_char(transaction_date, 'yyyy-MM') 
        ORDER BY revenue DESC) AS selling_product_per_month,
    product_id,
    sum(quantity_sold) OVER (
        PARTITION BY product_id, to_char(transaction_date, 'yyyy-MM')) AS quantity_per_product,
    sum(revenue) OVER (
        PARTITION BY product_id, to_char(transaction_date, 'yyyy-MM')) AS revenue_per_product
FROM sales_transactions
ORDER BY month, product_id;
