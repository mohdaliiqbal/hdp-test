## Task 2 - Data pipeline in Hive

##### Load employees database from launch pad.
There is a copy available in github so we will use that. Make sure you have git installed on your hive metastore host.  use the following command to clone the repository

    $ git clone https://github.com/datacharmer/test_db.git

change directory to test_db repository


    $ cd test_db

##### Load the employees database in the metastore mysql

    $ mysql -u hive -p < employees.sql
Enter the password you configured while installing the cluster.

Check data is loaded by running following commands one by one.

    $ mysql -u hive -p employees
    $ show databases

must show employees database

    $ show tables

must show few tables including employees and salaries

    exit # exit out of mysql

##### b.	Perform a full import of the ‘employees’ and ‘salaries’ tables into HDP
##### c.	Create Hive tables out of them using a suitable Storage Format
We will be using sqoop to load mysql database tables in to hive.

    sqoop import --connect jdbc:mysql://ip-10-0-0-159.ec2.internal:3306/employees --table employees --hive-import --username hive -P --num-mappers 8

    sqoop import --connect jdbc:mysql://ip-10-0-0-159.ec2.internal:3306/employees --table salaries --hive-import --username hive -P --num-mappers 8

##### d.	The ‘to_date’ column overlaps with the ‘from_date’ column in the ‘salaries’ table.  This results in the employee having two salary records on the day of the ‘to_date’ column. Fix it by decrementing the  ‘to_date’ column by one day (e.g. from ‘1987-06-18’ to ‘1987-06-17’) to make each salary record exclusive.

```
DROP TABLE IF EXISTS clean_salaries;

CREATE TEMPORARY TABLE clean_salaries(
       emp_no BIGINT, salary int,
       from_date STRING, to_date STRING
       )
   COMMENT 'Salary table'
   ROW FORMAT DELIMITED
   FIELDS TERMINATED BY 't'
   STORED AS RCFILE;

INSERT OVERWRITE TABLE clean_salaries select * from salaries where to_date <> '9999-01-01';

--Employees with overlapping records
DROP TABLE IF EXISTS salary_fixed_temp ;

CREATE TEMPORARY TABLE salary_fixed_temp AS Select x.emp_no, x.salary, x.from_date, date_sub(x.to_date, 1) as to_date from clean_salaries x JOIN clean_salaries y ON (x.emp_no = y.emp_no)
and x.to_date = y.from_date;

DROP TABLE IF EXISTS final_salaries;

CREATE TABLE final_salaries AS
select x.emp_no, x.salary, x.from_date,
CASE
  WHEN y.to_date IS NULL THEN x.to_date
  ELSE y.to_date
END
as to_date
FROM clean_salaries x LEFT OUTER JOIN salary_fixed_temp y ON (x.emp_no = y.emp_no AND x.from_date = y.from_date);

```

##### e.	The first salary record for an employee should reflect the day they joined the company.  However, the ‘employees.hire_date’ column doesn’t always reflect this.  Clean the data by replacing the ‘employees.hire_date’ column with the first salary record for an employee.

```
CREATE TEMPORARY TABLE employee_hire_date AS SELECT emp_no, MIN(from_date) as hire_date from salaries group by emp_no;

INSERT OVERWRITE TABLE employees
SELECT x.emp_no, x.birth_date, x.first_name, x.last_name, x.gender,
CASE
  WHEN y.hire_date is null then x.hire_date
  ELSE y.hire_date
END
FROM employees x LEFT OUTER JOIN employee_hire_date y on (x.emp_no = y.emp_no)
LIMIT 10;
```
````
10001	1953-09-02	Georgi	Facello	M	1986-06-26
10002	1964-06-02	Bezalel	Simmel	F	1996-08-03
10003	1959-12-03	Parto	Bamford	M	1995-12-03
10004	1954-05-01	Chirstian	Koblick	M	1986-12-01
10005	1955-01-21	Kyoichi	Maliniak	M	1989-09-12
10006	1953-04-20	Anneke	Preusig	F	1990-08-05
10007	1957-05-23	Tzvetan	Zielinski	F	1989-02-10
10008	1958-02-19	Saniya	Kalloufi	M	1998-03-11
10009	1952-04-19	Sumant	Peac	F	1985-02-18
10010	1963-06-01	Duangkaew	Piveteau	F	1996-11-24
````

##### g.Determine which employee lasted less than two weeks in the job in May 1985?

```
SELECT e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, e.hire_date from employees e JOIN (SELECT emp_no, min(from_date), max(to_date) FROM salaries group by emp_no having datediff(max(to_date), min(from_date)) < 14) s on (e.emp_no = s.emp_no )
where month(hire_date)=5 and year(hire_date) = 1985
```

#### output:
`296678	1955-12-16	Boutros	McClurg	M	1985-05-11`
