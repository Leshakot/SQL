
-- Пример решения задач на leetcode

SELECT request_at as Day, round(sum(if(status = "cancelled_by_driver" or status = "cancelled_by_client", 1,0 )) / COUNT(id) ,2) as "Cancellation Rate"
FROM Trips 
WHERE client_id  not in (SELECT users_id FROM Users  WHERE banned = "Yes") 
and driver_id   not in (SELECT users_id FROM Users  WHERE banned = "Yes")
and request_at between "2013-10-01" and "2013-10-03"
GROUP BY request_at 
ORDER BY 1;

--

SELECT MAX(salary) AS 'SecondHighestSalary'
FROM Employee
WHERE salary < (SELECT MAX(salary) 
                FROM Employee);

--

SELECT * 
FROM patients 
WHERE conditions LIKE '% DIAB1%' 
OR conditions LIKE 'DIAB1%';

--

delete p1 from person p1,person p2 
where p1.email=p2.email and p1.id>p2.id;

--

SELECT Users.name, 
       IFNULL(SUM(Rides.distance), 0)  travelled_distance
FROM Users
LEFT JOIN Rides  ON Users.id = Rides.user_id
GROUP BY Users.id
ORDER BY travelled_distance DESC, Users.name

--

SELECT user_id,
       INITCAP (name) AS name 
FROM Users
ORDER BY  user_id

--

SELECT Employee.name  AS Employee 
FROM Employee
JOIN Employee AS e ON e.id  = Employee.managerId  
WHERE Employee.salary   > e.salary 

--

SELECT name 
FROM Customer
WHERE referee_id <> 2 OR referee_id is null

--
SELECT customer_number
FROM Orders
GROUP BY customer_number 
ORDER BY COUNT(customer_number) DESC
LIMIT 1

--
SELECT name 
FROM SalesPerson
WHERE sales_id NOT IN (
        SELECT Orders.sales_id 
        FROM Orders
        JOIN Company ON Company.com_id  = Orders.com_id 
        WHERE Company.name = 'RED')

--

SELECT email
FROM Person 
GROUP BY email  
HAVING count(email) > 1

--

SELECT user_id, COUNT(user_id) AS followers_count
FROM Followers 
GROUP BY user_id 
ORDER BY user_id

--

SELECT name, population, area
FROM World
WHERE area > 3000000 OR population >= 25000000

--

SELECT *
FROM Cinema
WHERE description  <> 'boring' AND id % 2 = 1
ORDER BY rating DESC

--

SELECT DISTINCT author_id AS id
FROM Views
WHERE article_id IN (
SELECT article_id  AS id
FROM Views 
WHERE author_id = viewer_id 
GROUP BY article_id )
ORDER BY id

--

SELECT DISTINCT author_id AS id
FROM Views
WHERE article_id IN (
SELECT article_id  AS id
FROM Views 
WHERE author_id = viewer_id 
GROUP BY article_id )
ORDER BY id

--

--

SELECT employee_id 
FROM Salaries 
WHERE employee_id NOT IN (SELECT employee_id 
                             FROM Employees
                           )
    UNION
SELECT employee_id 
FROM Employees 
WHERE employee_id NOT IN (SELECT employee_id 
                             FROM Salaries
                           )
ORDER BY 1

--

SELECT player_id, 
       CAST(MIN(event_date) AS date) AS first_login 
FROM Activity
GROUP BY player_id 

--
