
-- Базовый SQL
-- Срезы данных в SQL, агрегирующие функции, подзапросы и временные таблицы

SELECT funding_total
FROM company
WHERE category_code = 'news' AND country_code = 'USA'
ORDER BY funding_total DESC 

---

SELECT SUM(price_amount)
    FROM acquisition
    WHERE term_code = 'cash'
    AND EXTRACT(YEAR  FROM CAST(acquired_at AS timestamp)) BETWEEN 2011 AND 2013;
    
---

SELECT *
FROM people
WHERE twitter_username LIKE '%money%' AND last_name LIKE 'K%'

---

SELECT *,
    CASE
        WHEN invested_companies > 100 THEN 'high_activity'
        WHEN invested_companies < 20 THEN 'low_activity'
        ELSE 'middle_activity'
    END
FROM fund

---

SELECT country_code,
       MIN(invested_companies), 
       MAX(invested_companies),
       AVG(invested_companies)
FROM fund
WHERE EXTRACT(YEAR FROM CAST(founded_at AS date)) BETWEEN 2010 AND 2012
GROUP BY country_code
HAVING MIN(invested_companies) <> 0
ORDER BY avg DESC, country_code
LIMIT 10;

---

SELECT DISTINCT(c.name)
FROM company AS c
LEFT JOIN funding_round AS fr ON c.id=fr.company_id 
WHERE status = 'closed' AND is_first_round=1 AND is_last_round =1

---

WITH 
i AS (SELECT e.person_id AS ttt,
     COUNT(instituition) AS st
     FROM people AS pl
     LEFT JOIN education AS e ON pl.id=e.person_id 
     GROUP BY e.person_id
        ), 
c AS (SELECT p.id AS t
      FROM people AS p
      INNER JOIN company AS c ON c.id=p.company_id
      INNER JOIN funding_round AS fr ON c.id=fr.company_id 
      RIGHT JOIN education AS e ON p.id = e.person_id
      WHERE status = 'closed' AND is_first_round=1 AND is_last_round =1 AND  e.instituition IS NOT NULL
      GROUP BY t)
SELECT c.t,
       i.st
FROM i right OUTER JOIN c ON i.ttt=c.t

---

WITH 
-- первый подзапрос с псевдонимом i
i AS (SELECT id AS nam1,
        name AS nam2
        FROM company
        WHERE milestones  > 6
        ), 
-- второй подзапрос с псевдонимом c
 c AS (SELECT company_id  AS fun1, 
        raised_amount AS fun2
        FROM funding_round
        WHERE EXTRACT(YEAR FROM CAST(funded_at AS date)) BETWEEN '2012' AND '2013'
            )

SELECT *
FROM i LEFT OUTER JOIN c ON i.nam1=c.fun1

---

WITH
a AS(SELECT id,
            name
     FROM company
     WHERE category_code = 'social'),
b AS(SELECT company_id,
            raised_amount,
            funded_at
    FROM funding_round)
SELECT a.name,
       --b.raised_amount,
       EXTRACT(MONTH FROM CAST(funded_at AS date)) AS month
FROM a
LEFT JOIN b ON a.id=b.company_id
WHERE b.raised_amount <> 0 
      AND (EXTRACT(YEAR FROM CAST(b.funded_at AS date)) BETWEEN '2010' AND '2013')
      
---

WITH 
a AS (SELECT EXTRACT(MONTH  FROM CAST(acquired_at AS date)) AS month,
             COUNT(a.acquired_company_id) AS company, 
             SUM(a.price_amount) AS sum
      FROM acquisition AS a
      WHERE EXTRACT(YEAR FROM CAST(acquired_at AS date)) BETWEEN '2010' AND '2013'
      GROUP BY EXTRACT(MONTH  FROM CAST(acquired_at AS date))
     ), 
b AS (SELECT EXTRACT(MONTH FROM CAST(fr.funded_at AS date)) AS month,
             COUNT(DISTINCT fund.name) AS funder
      FROM funding_round AS fr
      LEFT JOIN investment ON investment.funding_round_id = fr.id
      LEFT JOIN fund ON investment.fund_id  = fund.id
      WHERE (EXTRACT(YEAR FROM CAST(fr.funded_at AS date)) BETWEEN '2010' AND '2013')
            AND (country_code = 'USA') 
      GROUP BY EXTRACT(MONTH FROM CAST(fr.funded_at AS date)) 
)
SELECT a.month,
       b.funder,
       a.company,
       a.sum
FROM a
LEFT JOIN b ON a.month = b.month;

---

WITH 
a AS (SELECT country_code AS code1,
          AVG(funding_total) AS avg_11
      FROM company
      WHERE EXTRACT(YEAR FROM CAST(founded_at AS date)) = '2011'
      GROUP BY country_code
     ),
b AS (SELECT country_code AS code,
             AVG(funding_total) AS avg_12
      FROM company
      WHERE EXTRACT(YEAR FROM CAST(founded_at AS date)) = '2012'
      GROUP BY country_code
     ),
c AS (SELECT country_code AS code,
             AVG(funding_total) AS avg_13
      FROM company
      WHERE EXTRACT(YEAR FROM CAST(founded_at AS date)) = '2013'
      GROUP BY country_code
     )
SELECT a.code1,
       a.avg_11,
       b.avg_12,
       c.avg_13
FROM a
INNER JOIN b ON a.code1=b.code
INNER JOIN c on a.code1=c.code
ORDER BY  a.avg_11 DESC

    
