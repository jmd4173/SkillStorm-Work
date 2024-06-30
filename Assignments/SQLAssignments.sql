

-- Ex 1 Start
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id INT IDENTITY PRIMARY KEY,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BIT DEFAULT 0,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BIT DEFAULT 0,
    created_dt DATE DEFAULT GETDATE()
);
-- Ex 1 Finish


-- Ex 2 Start
USE Day1
DROP TABLE IF EXISTS courses;
CREATE TABLE courses (
	course_id INT IDENTITY(1,1) PRIMARY KEY,
	course_name VARCHAR(60),
	course_author VARCHAR(40),
	course_status VARCHAR(20) NOT NULL CHECK (course_status IN ('published','draft','inactive')),
	course_published_dt DATE DEFAULT NULL,
);
INSERT INTO courses (course_name, course_author,course_status,course_published_dt)
VALUES ('Programming using Python',	'Bob Dillon',	'published',	'2020-09-30'),
		('Data Engineering using Python',	'Bob Dillon',	'published',	'2020-07-15'),
		('Data Engineering using Scala',	'Elvis Presley'	,'draft', NULL),
		('Programming using Scala',	'Elvis Presley'	,'published',	'2020-05-12'),
		('Programming using Java',	'Mike Jack',	'inactive',	'2020-08-10'),
		('Web Applications - Python Flask','	Bob Dillon'	,'inactive'	,'2020-07-20'),
		('Web Applications - Java Spring',	'Mike Jack',	'draft',	NULL),
		('Pipeline Orchestration - Python',	'Bob Dillon',	'draft',	NULL),
		('Streaming Pipelines - Python',	'Bob Dillon',	'published',	'2020-10-05'),
		('Web Applications - Scala Play',	'Elvis Presley'	,'inactive'	,'2020-09-30'),
		('Web Applications - Python Django',	'Bob Dillon',	'published',	'2020-06-23'),
		('Server Automation - Ansible',	'Uncle Sam'	,'published'	,'2020-07-05');

UPDATE 
courses 
SET 
	course_status = 'published'
WHERE 
	course_status LIKE 'draft' 
AND
	(course_name LIKE '%Python%'
OR
	course_name LIKE '%Scala%');



DELETE FROM 
	courses 
WHERE 
	course_status NOT LIKE 'published'
AND
	course_status NOT LIKE 'draft';


SELECT 
	course_author, 
	count(1) AS course_count
FROM 
	courses
WHERE 
	course_status= 'published'
GROUP BY 
	course_author;

-- Ex 2 Finish


-- Ex 3 Start

/* Ex3 Pt1-------------------------------------------------*/
USE retail_db
SELECT 
    customers.customer_id, 
    customers.customer_fname,
	customers.customer_lname,
    COUNT(*) AS Order_Count 
FROM 
    customers
JOIN 
    orders
ON 
    customers.customer_id = orders.order_customer_id
GROUP BY 
    customers.customer_id, 
    customers.customer_fname,
	customers.customer_lname
ORDER BY
	customers.customer_id ASC,
	Order_Count DESC;
/* Ex3 Pt1-------------------------------------------------*/



/* Ex3 Pt2-------------------------------------------------*/
SELECT 
    DISTINCT customers.*
FROM 
    customers
JOIN 
    orders
ON 
    customers.customer_id = orders.order_customer_id
WHERE
	orders.order_date BETWEEN '2014-01-01 00:00:00:000' AND '2014-01-31 00:00:00:000'
	AND
	orders.order_id IS NOT NULL
ORDER BY
	customers.customer_id ASC;
/* Ex3 Pt2-------------------------------------------------*/


/* Ex3 Pt3-------------------------------------------------*/
SELECT 
	customers.customer_id, 
	customers.customer_fname, 
	customers.customer_lname,
	COALESCE(SUM(order_items.order_item_subtotal), 0) Revenue_Per_Customer
FROM 
    customers
JOIN 
    orders
ON 
    customers.customer_id = orders.order_customer_id
LEFT JOIN 
	order_items
ON
	orders.order_id = order_items.order_item_order_id
WHERE
	orders.order_date BETWEEN '2014-01-01 00:00:00:000' AND '2014-01-31 00:00:00:000'
	AND
	orders.order_id IS NOT NULL
	AND
	orders.order_status IN ('COMPLETE','CLOSED')
GROUP BY 
	customers.customer_id,
	customers.customer_fname,
	customers.customer_lname
ORDER BY
	Revenue_Per_Customer DESC,
	customers.customer_id ASC;
/* Ex3 Pt3-------------------------------------------------*/


/* Ex3 Pt4-------------------------------------------------*/
SELECT * FROM categories



SELECT 
	categories.*,
	COALESCE(SUM(order_item_product_price),0) category_revunue
FROM 
    orders
left JOIN 
	order_items
ON
	orders.order_id = order_items.order_item_order_id
JOIN 
	products
ON
	order_items.order_item_id = products.product_id
left JOIN
	categories
ON
	products.product_category_id = categories.category_id
WHERE
	orders.order_status IN ('COMPLETE','CLOSED')
GROUP BY
	categories.category_id,
	categories.category_department_id,
	categories.category_name
ORDER BY 
	categories.category_id ASC;
/* Ex3 Pt4-------------------------------------------------*/

/* Ex3 Pt5-------------------------------------------------*/
SELECT 
	departments.*,
	count(products.product_id) product_count
FROM
	departments
LEFT JOIN
	categories
ON
	departments.department_id = categories.category_department_id
LEFT JOIN
	products
ON
	categories.category_id = products.product_category_id
GROUP BY
	departments.department_id,
	departments.department_name
ORDER BY
	departments.department_id ASC;
/* Ex3 Pt5-------------------------------------------------*/

-- Ex 3 Finish

-- Ex 4 Start

/* Ex4 Pt1-------------------------------------------------*/

USE retail_db

SELECT
	(SELECT MAX(order_id) FROM orders) [Order ID],
	(SELECT MAX(order_item_id) FROM order_items) [Order Items ID],
	(SELECT MAX(customer_id) FROM customers) [Customer ID], 
	(SELECT MAX(product_id) FROM products) [Product ID], 
	(SELECT MAX(category_id) FROM categories) [Category ID], 
	(SELECT MAX(department_id) FROM departments) [Department ID];



ALTER TABLE orders
ADD CONSTRAINT order_customer_id_FK FOREIGN KEY (order_customer_id) REFERENCES customers(customer_id);

ALTER TABLE order_items
ADD CONSTRAINT order_items_order_id_FK FOREIGN KEY (order_item_order_id) REFERENCES orders(order_id);

ALTER TABLE order_items
ADD CONSTRAINT order_items_product_id_FK FOREIGN KEY (order_item_product_id) REFERENCES products(product_id);


SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE;






/* Ex4 Pt1-------------------------------------------------*/


-- Ex 4 Finish




-- Ex 5 Start
USE retail_db;
DROP TABLE IF EXISTS orders_part;

DROP PARTITION FUNCTION functioncall;
CREATE PARTITION FUNCTION functioncall(DATETIME)
AS RANGE RIGHT FOR VALUES (
    '20140101'
	);
CREATE PARTITION SCHEME myrange
AS PARTITION functioncall
TO ([PRIMARY],
	[partition20140101]
);



CREATE TABLE orders_part(
	order_id INT IDENTITY NOT NULL,
	order_date DATETIME NOT NULL,
	order_customer_id INT NOT NULL,
	order_status VARCHAR(45) NOT NULL,
	PRIMARY KEY (order_id, order_date)
) ON myrange(order_date);

-- import info from orders to orders_part
SET IDENTITY_INSERT orders_part ON
INSERT INTO orders_part (
	order_id,
	order_date,
	order_customer_id,
	order_status)
SELECT 
	orders.order_id,
	orders.order_date,
	orders.order_customer_id,
	orders.order_status
FROM 
	orders;



-- Ex 5 Finish


-- Ex 6 Start
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id int PRIMARY KEY IDENTITY,
    user_first_name VARCHAR(30),
    user_last_name VARCHAR(30),
    user_email_id VARCHAR(50),
    user_gender VARCHAR(1),
    user_unique_id VARCHAR(15),
    user_phone_no VARCHAR(20),
    user_dob DATE,
    created_ts DATETIME
);
insert into users (
    user_first_name, user_last_name, user_email_id, user_gender, 
    user_unique_id, user_phone_no, user_dob, created_ts
) VALUES
    ('Giuseppe', 'Bode', 'gbode0@imgur.com', 'M', '88833-8759', 
     '+86 (764) 443-1967', '1973-05-31', '2018-04-15 12:13:38'),
    ('Lexy', 'Gisbey', 'lgisbey1@mail.ru', 'N', '262501-029', 
     '+86 (751) 160-3742', '2003-05-31', '2020-12-29 06:44:09'),
    ('Karel', 'Claringbold', 'kclaringbold2@yale.edu', 'F', '391-33-2823', 
     '+62 (445) 471-2682', '1985-11-28', '2018-11-19 00:04:08'),
    ('Marv', 'Tanswill', 'mtanswill3@dedecms.com', 'F', '1195413-80', 
     '+62 (497) 736-6802', '1998-05-24', '2018-11-19 16:29:43'),
    ('Gertie', 'Espinoza', 'gespinoza4@nationalgeographic.com', 'M', '471-24-6869', 
     '+249 (687) 506-2960', '1997-10-30', '2020-01-25 21:31:10'),
    ('Saleem', 'Danneil', 'sdanneil5@guardian.co.uk', 'F', '192374-933', 
     '+63 (810) 321-0331', '1992-03-08', '2020-11-07 19:01:14'),
    ('Rickert', 'O''Shiels', 'roshiels6@wikispaces.com', 'M', '749-27-47-52', 
     '+86 (184) 759-3933', '1972-11-01', '2018-03-20 10:53:24'),
    ('Cybil', 'Lissimore', 'clissimore7@pinterest.com', 'M', '461-75-4198', 
     '+54 (613) 939-6976', '1978-03-03', '2019-12-09 14:08:30'),
    ('Melita', 'Rimington', 'mrimington8@mozilla.org', 'F', '892-36-676-2', 
     '+48 (322) 829-8638', '1995-12-15', '2018-04-03 04:21:33'),
    ('Benetta', 'Nana', 'bnana9@google.com', 'N', '197-54-1646', 
     '+420 (934) 611-0020', '1971-12-07', '2018-10-17 21:02:51'),
    ('Gregorius', 'Gullane', 'ggullanea@prnewswire.com', 'F', '232-55-52-58', 
     '+62 (780) 859-1578', '1973-09-18', '2020-01-14 23:38:53'),
    ('Una', 'Glayzer', 'uglayzerb@pinterest.com', 'M', '898-84-336-6', 
     '+380 (840) 437-3981', '1983-05-26', '2019-09-17 03:24:21'),
    ('Jamie', 'Vosper', 'jvosperc@umich.edu', 'M', '247-95-68-44', 
     '+81 (205) 723-1942', '1972-03-18', '2020-07-23 16:39:33'),
    ('Calley', 'Tilson', 'ctilsond@issuu.com', 'F', '415-48-894-3', 
     '+229 (698) 777-4904', '1987-06-12', '2020-06-05 12:10:50'),
    ('Peadar', 'Gregorowicz', 'pgregorowicze@omniture.com', 'M', '403-39-5-869', 
     '+7 (267) 853-3262', '1996-09-21', '2018-05-29 23:51:31'),
    ('Jeanie', 'Webling', 'jweblingf@booking.com', 'F', '399-83-05-03', 
     '+351 (684) 413-0550', '1994-12-27', '2018-02-09 01:31:11'),
    ('Yankee', 'Jelf', 'yjelfg@wufoo.com', 'F', '607-99-0411', 
     '+1 (864) 112-7432', '1988-11-13', '2019-09-16 16:09:12'),
    ('Blair', 'Aumerle', 'baumerleh@toplist.cz', 'F', '430-01-578-5', 
     '+7 (393) 232-1860', '1979-11-09', '2018-10-28 19:25:35'),
    ('Pavlov', 'Steljes', 'psteljesi@macromedia.com', 'F', '571-09-6181', 
     '+598 (877) 881-3236', '1991-06-24', '2020-09-18 05:34:31'),
    ('Darn', 'Hadeke', 'dhadekej@last.fm', 'M', '478-32-02-87', 
     '+370 (347) 110-4270', '1984-09-04', '2018-02-10 12:56:00'),
    ('Wendell', 'Spanton', 'wspantonk@de.vu', 'F', null, 
     '+84 (301) 762-1316', '1973-07-24', '2018-01-30 01:20:11'),
    ('Carlo', 'Yearby', 'cyearbyl@comcast.net', 'F', null, 
     '+55 (288) 623-4067', '1974-11-11', '2018-06-24 03:18:40'),
    ('Sheila', 'Evitts', 'sevittsm@webmd.com', null, '830-40-5287',
     null, '1977-03-01', '2020-07-20 09:59:41'),
    ('Sianna', 'Lowdham', 'slowdhamn@stanford.edu', null, '778-0845', 
     null, '1985-12-23', '2018-06-29 02:42:49'),
    ('Phylys', 'Aslie', 'paslieo@qq.com', 'M', '368-44-4478', 
     '+86 (765) 152-8654', '1984-03-22', '2019-10-01 01:34:28');



SELECT * FROM USERS;


/* Ex6 Pt1-------------------------------------------------*/
SELECT 
	 YEAR(created_ts) created_year,
	 COUNT(YEAR(created_ts)) user_count
from 
	users
GROUP BY
	YEAR(created_ts)
/* Ex6 Pt1-------------------------------------------------*/

/* Ex6 Pt2-------------------------------------------------*/
SELECT 
	users.user_id,
	users.user_dob,
	users.user_email_id,
	DATENAME(WEEKDAY, users.user_dob) user_day_of_birth
FROM
	users
/* Ex6 Pt2-------------------------------------------------*/

/* Ex6 Pt3-------------------------------------------------*/
SELECT
	users.user_id,
	UPPER(CONCAT(users.user_first_name,' ', users.user_first_name)) user_name,
	users.user_email_id,
	users.created_ts,
	YEAR(users.created_ts) created_year
FROM 
	users


/* Ex6 Pt3-------------------------------------------------*/

/* Ex6 Pt4-------------------------------------------------*/
SELECT
	COALESCE(
		CASE 
			WHEN users.user_gender LIKE 'm' THEN 'Male' 
			WHEN users.user_gender LIKE 'F' THEN 'Female'
			WHEN users.user_gender LIKE 'N' THEN 'Not Specified'
			END,
	'Non-Binary') user_gender,
	COUNT(*) user_count
FROM
	users
GROUP BY
	users.user_gender;

/* Ex6 Pt4-------------------------------------------------*/

/* Ex6 Pt5-------------------------------------------------*/
SELECT 
	users.user_id,
	users.user_unique_id,
	CASE
		WHEN LEN((REPLACE(users.user_unique_id,'-','' ))) >= 9 THEN COALESCE (RIGHT (REPLACE(users.user_unique_id,'-','' ), 4), 'Not Specified') 
		WHEN users.user_unique_id IS null THEN 'Not Specified'
		ELSE 'Invalid Unique Id'
	END
FROM
	users

/* Ex6 Pt5-------------------------------------------------*/

/* Ex6 Pt6-------------------------------------------------*/

SELECT * FROM USERS;

SELECT 
	REPLACE((SUBSTRING(users.user_phone_no,0,CHARINDEX('(',users.user_phone_no ))),'+','') as country_code,
	COUNT(*) user_count
FROM
	users
GROUP BY
	REPLACE((SUBSTRING(users.user_phone_no,0,CHARINDEX('(',users.user_phone_no ))),'+','')
HAVING
	REPLACE((SUBSTRING(users.user_phone_no,0,CHARINDEX('(',users.user_phone_no ))),'+','') IS NOT NULL
ORDER BY
	CAST(REPLACE((SUBSTRING(users.user_phone_no,0,CHARINDEX('(',users.user_phone_no ))),'+','') AS INT)  ASC
	
/* Ex6 Pt6-------------------------------------------------*/

/* Ex6 Pt7-------------------------------------------------*/
SELECT 
	COUNT(*) count
FROM
	order_items
WHERE
	(order_item_subtotal NOT LIKE (order_item_quantity * order_item_product_price))


/* Ex6 Pt7-------------------------------------------------*/

/* Ex6 Pt8-------------------------------------------------*/
SELECT * FROM orders


SELECT 
	DayType, 
	OrderCount
FROM
(
	SELECT
		COUNT(CASE WHEN DATENAME(WEEKDAY, order_date) IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') THEN 1 ELSE NULL END) AS Weekday,
		COUNT(CASE WHEN DATENAME(WEEKDAY, order_date) IN ('Saturday', 'Sunday') THEN 1 ELSE NULL END) AS Weekend
	FROM 
		orders
	WHERE 
		orders.order_date BETWEEN '2014-01-01 00:00:00:000' AND '2014-01-31 00:00:00:000'
) SUB
UNPIVOT
(
	OrderCount for DayType IN
	([Weekday], [Weekend])
) DisplayResults;



/* Ex6 Pt8-------------------------------------------------*/


-- Ex6 Finish


--Exercise 1: Simple Subquery
--Retrieve the names of categories that have more than 5 products.

--Ex7 Start


/* Ex7 Pt1-------------------------------------------------*/

SELECT * FROM categories

SELECT * FROM products


SELECT 
	categories.category_name
FROM 
	categories
	WHERE 
		categories.category_id IN 
	(
	SELECT 
		products.product_category_id
	FROM 
		products
	GROUP BY
		products.product_category_id
	HAVING 
		COUNT(*) > 5
	);

/* Ex7 Pt1-------------------------------------------------*/


/* Ex7 Pt2-------------------------------------------------*/

SELECT * FROM orders
use retail_db;

SELECT 
	customers.customer_fname,
	customers.customer_lname
FROM
	customers
WHERE customers.customer_id IN
	(
	SELECT 
		orders.order_customer_id
	FROM 
		orders
	GROUP BY
		order_customer_id
	HAVING 
		COUNT(*) > 10
	);

/* Ex7 Pt2-------------------------------------------------*/



/* Ex7 Pt3-------------------------------------------------*/
SELECT * FROM products

SELECT * FROM orders

SELECT
	products.product_name,
	(SELECT AVG(products.product_price) Avg_price
FROM 
	orders
JOIN
	order_items
ON
	orders.order_id = order_items.order_item_order_id
JOIN
	products
ON
	order_items.order_item_product_id = products.product_id

WHERE order_date BETWEEN '2013-10-01 00:00:00:000' AND '2013-10-31 00:00:00:000'
) as AVGTable
FROM 
	orders
JOIN
	order_items
ON
	orders.order_id = order_items.order_item_order_id
JOIN
	products
ON
	order_items.order_item_product_id = products.product_id
WHERE order_date BETWEEN '2013-10-01 00:00:00:000' AND '2013-10-31 00:00:00:000'



/* Ex7 Pt3-------------------------------------------------*/


-- This looks terrible to me
-- If I have extra time Im going to fix this

/* Ex7 Pt4-------------------------------------------------*/

SELECT 
	orders.*,
	order_items.order_item_subtotal
FROM 
	orders
JOIN 
	order_items
ON
	orders.order_id = order_items.order_item_order_id
WHERE 
order_items.order_item_subtotal > (
SELECT
	AVG(order_item_subtotal)
FROM 
	orders
JOIN
	order_items
ON
	orders.order_id = order_items.order_item_order_id) 

/* Ex7 Pt4-------------------------------------------------*/

/* Ex7 Pt5-------------------------------------------------*/


WITH CTE (category_name, total) as (
SELECT 
	categories.category_name,
	COUNT(categories.category_department_id) total
FROM 
	categories
JOIN
	products
ON
	categories.category_id = products.product_category_id 
GROUP BY 
	categories.category_department_id,
	categories.category_name

)
SELECT TOP 3 * FROM CTE 
ORDER BY total DESC;

/* Ex7 Pt5-------------------------------------------------*/

/* Ex7 Pt6-------------------------------------------------*/

WITH customer_average ([customer_id], [customer_fname], [customer_lname], [avg_customer]) AS 
(
	SELECT 
		customer_id, 
		customers.customer_fname,
		customers.customer_lname,
		avg(order_item_subtotal) cust_avg 
	FROM 
		customers
	JOIN 
		orders
	ON	
		customers.customer_id = order_customer_id
	JOIN 
		order_items
	ON 
		orders.order_id = order_items.order_item_order_id
	WHERE 
		MONTH(order_date) = 12
	GROUP BY 
		customer_id,
		customers.customer_fname,
		customers.customer_lname
)
SELECT 
	* 
FROM 
	customer_average 
WHERE 
	avg_customer > (	
		SELECT 
			CAST(ROUND(avg(order_item_subtotal),2) AS DECIMAL (10,2)) cust_avg FROM customers
			JOIN
				orders
			ON 
				customers.customer_id = order_customer_id
			JOIN 
				order_items
			ON 
				orders.order_id = order_items.order_item_order_id
			WHERE 
				MONTH(order_date) = 12)
ORDER BY 
	avg_customer DESC;

/*
all_average (avg_all_customer) AS 
(
SELECT avg(order_item_subtotal) cust_avg FROM customers
JOIN orders
ON 
customers.customer_id = order_customer_id
JOIN order_items
ON 
orders.order_id = order_items.order_item_order_id
WHERE MONTH(order_date) = 12
)*/

/* Ex7 Pt6-------------------------------------------------*/

--Ex7 Finish





--Ex8 Start

/* Ex8 Pt1-------------------------------------------------*/

use hr_db;
SELECT * FROM departments;

		
WITH department (did, avgsal) AS
(
SELECT
		departments.department_id,
		CAST(avg(employees.salary) AS DECIMAL (10,2)) avg_salary_expense
	FROM
		employees 
	JOIN	 
		departments
	ON
		employees.department_id = departments.department_id
	GROUP BY 
		departments.department_id
)
SELECT 
	employee_id, 
	department_name,
	e1.salary,
	department.avgsal avg_salary_expense
FROM 
	employees e1
JOIN 
	departments
ON
	e1.department_id = departments.department_id
JOIN
	department
ON
	e1.department_id = department.did
WHERE e1.salary > (
	SELECT
		CAST(avg(e2.salary) AS DECIMAL (10,2)) avg_salary_expense
	FROM
		employees e2
	JOIN	 
		departments
	ON
		e2.department_id = departments.department_id
	WHERE 
		e1.department_id = e2.department_id
	GROUP BY 
		departments.department_id
)  
ORDER BY 
	departments.department_id ASC,
	e1.salary DESC;
/* Ex8 Pt1-------------------------------------------------*/

/* Ex8 Pt2-------------------------------------------------*/
use hr_db;

SELECT 
	employees.employee_id, 
	departments.department_name,
	employees.salary,
	SUM(employees.salary) OVER (PARTITION BY employees.department_id ORDER BY salary ASC) cum_salary_expense
FROM 
	employees
JOIN
	departments 
ON
	employees.department_id = departments.department_id
WHERE 
	department_name IN ('IT', 'Finance')
ORDER BY
	department_name ASC,
	salary ASC;
/* Ex8 Pt2-------------------------------------------------*/

/* Ex8 Pt3-------------------------------------------------*/
with CTE (depname, empid,empsalary,depid, denserank) AS
(
	SELECT 
		department_name, 
		employee_id, 
		salary, 
		departments.department_id,
		DENSE_RANK() OVER (PARTITION BY department_name ORDER BY salary DESC)
	FROM 
		employees
	JOIN 
		departments
	ON
		employees.department_id = departments.department_id
	GROUP BY 
		employees.department_id,
		department_name, 
		employee_id, 
		employees.salary,
		departments.department_id
)
SELECT 
	empid, 
	depid, 
	depname, 
	empsalary, 
	denserank 
FROM 
	CTE
WHERE 
	denserank < 4
ORDER BY 
	CTE.depid ASC, 
	CTE.empsalary DESC, 
	CTE.denserank ASC;

/* Ex8 Pt3-------------------------------------------------*/

/* Ex8 Pt4-------------------------------------------------*/
use retail_db;

WITH CTE (pid,pname,prevenue) AS 
(
SELECT 
	product_id, 
	product_name, 
	CAST(SUM(order_item_subtotal) AS DECIMAL (10,2)) revenue
FROM 
	orders
JOIN 
	order_items
ON
	orders.order_id = order_items.order_item_order_id
JOIN 
	products
ON
	order_items.order_item_product_id = products.product_id
WHERE 
	order_status IN ('CLOSED','COMPLETE')
AND 
	order_date BETWEEN '2014-01-01 00:00:00:000' AND '2014-01-31 00:00:00:000'
GROUP BY 
	products.product_id, 
	product_name
) 
SELECT TOP 3
	pid product_id,
	pname product_name,
	prevenue revenue,
	DENSE_RANK() OVER (ORDER BY prevenue DESC) product_rank
FROM CTE ORDER BY prevenue DESC

/* Ex8 Pt4-------------------------------------------------*/

/* Ex8 Pt5-------------------------------------------------*/
WITH CTE (cid, cname, pid, pname, prevenue) AS
(
SELECT 
	category_id,
	category_name,
	product_id, 
	product_name, 
	CAST(SUM(order_item_subtotal) AS DECIMAL (10,2)) revenue
FROM 
	orders
JOIN 
	order_items
ON
	orders.order_id = order_items.order_item_order_id
JOIN 
	products
ON
	order_items.order_item_product_id = products.product_id
JOIN
	categories
ON
	products.product_category_id = categories.category_id
WHERE 
	order_status IN ('CLOSED','COMPLETE')
AND 
	order_date BETWEEN '2014-01-01 00:00:00:000' AND '2014-01-31 00:00:00:000'
AND
	category_name IN ('Cardio Equipment','Strength Training')
GROUP BY 
	products.product_id, 
	product_name,
	category_id,
	category_name
) SELECT 
	cid category_id, 
	cname category_name, 
	pid product_id, 
	pname product_name, 
	prevenue revenue,
	DENSE_RANK() OVER (PARTITION BY cname ORDER BY prevenue DESC) product_rank
FROM 
	CTE	
ORDER BY
	cid ASC,
	prevenue DESC


/* Ex8 Pt5-------------------------------------------------*/

--Ex8 Finish




--Ex9 Start

/* Ex9 Pt1-------------------------------------------------*/
USE AdventureWorks2022;
SELECT 
	ProductID, Name 
FROM 
	Production.Product;


SELECT * FROM HumanResources.Employee


-- 




--



SELECT * 
FROM Sales.SalesOrderHeader 
WHERE CustomerID = 295;

/* Ex9 Pt1-------------------------------------------------*/


--Ex9 Finish



