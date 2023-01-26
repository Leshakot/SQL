
--Оконные функции, расчет бизнес показателей, кагорный анализ

WITH fd AS (
SELECT DISTINCT
       user_id,
       FIRST_VALUE(creation_date) OVER(PARTITION BY user_id ORDER BY creation_date) AS first_date
FROM stackoverflow.posts AS p)
SELECT fd.user_id,
       fd.first_date - u.creation_date 
FROM fd
JOIN stackoverflow.users AS u ON fd.user_id = u.id

---

SELECT DISTINCT EXTRACT(DAY FROM creation_date::date) AS nov_day,
       COUNT(id) OVER (PARTITION BY EXTRACT(DAY FROM creation_date::date)) AS users_day_cnt,
       COUNT(id) OVER (ORDER BY EXTRACT(DAY FROM creation_date::date)) AS cum_sum
FROM stackoverflow.users
WHERE creation_date ::date BETWEEN '2008-11-01' AND '2008-11-30'

---

WITH users AS (
    SELECT COUNT(id),
           EXTRACT(DAY FROM creation_date::date) AS dr
    FROM stackoverflow.users
    WHERE DATE_TRUNC('month', creation_date) BETWEEN '2008-11-01' AND '2008-11-30'
    GROUP BY dr)
SELECT dr,
       count,
       SUM(count) OVER (ORDER BY dr)
FROM users

---

WITH top AS (
    SELECT id,
           views,
           CASE
               WHEN views < 100 THEN 3
               WHEN views >= 100 AND views < 350 THEN 2
               WHEN views >= 350 THEN 1       
           END AS rank
    FROM stackoverflow.users
    WHERE location LIKE '%United States%' AND views <> 0),
tps AS (SELECT rank,
           MAX(views) AS max_views
    FROM top AS t
    GROUP BY rank)
SELECT DISTINCT u.id,
       tps.rank,
       tps.max_views
FROM tps
LEFT JOIN stackoverflow.users AS u ON u.views = tps.max_views
WHERE location LIKE '%United States%'
ORDER BY max_views DESC, id


---

WITH b AS (
SELECT user_id,
       COUNT(user_id) AS count_pin
FROM stackoverflow.badges
GROUP BY user_id
HAVING   COUNT(user_id) > 1000)
SELECT title
FROM b
JOIN stackoverflow.posts AS p on p.user_id = b.user_id
WHERE title IS NOT NULL

---

WITH t AS (
SELECT DISTINCT
        user_id,
        COUNT(user_id) OVER(PARTITION BY user_id)
FROM stackoverflow.badges
WHERE creation_date :: date BETWEEN '2008-11-15' AND '2008-12-15' 
ORDER BY count DESC)
SELECT *,
       DENSE_RANK() OVER(ORDER BY count DESC)
FROM t
ORDER BY count DESC, user_id
LIMIT 10

---

WITH badges AS (
SELECT user_id,
       COUNT(*)
FROM stackoverflow.badges
WHERE creation_date ::date BETWEEN '2008-11-15' AND '2008-12-15'
GROUP BY user_id)
SELECT *,
      DENSE_RANK() OVER(ORDER BY count DESC)
FROM badges
ORDER BY count DESC, user_id
LIMIT 10; 

---

SELECT DISTINCT 
       SUM(views_count) OVER(PARTITION BY  DATE_TRUNC('month', creation_date) ORDER BY  DATE_TRUNC('month', creation_date)),
       DATE_TRUNC('month', creation_date) :: date
FROM stackoverflow.posts
WHERE  DATE_TRUNC('month', creation_date) BETWEEN '2008-01-01' AND '2008-12-01'
ORDER BY sum DESC

---

SELECT display_name, 
       COUNT(DISTINCT p.user_id)
FROM stackoverflow.posts AS p
JOIN stackoverflow.users AS u ON u.id = p.user_id
WHERE p.post_type_id IN (SELECT id 
                         FROM stackoverflow.post_types 
                         WHERE type = 'Answer')       
       AND p.creation_date::date <= u.creation_date::date + INTERVAL '1 month' 
GROUP BY display_name
HAVING COUNT(p.user_id) > 100
ORDER BY display_name 

---

WITH t AS (
    SELECT u.id
    FROM stackoverflow.users AS u
    JOIN stackoverflow.posts AS p ON p.user_id = u.id
    WHERE  DATE_TRUNC('month', u.creation_date) :: date = '2008-09-01' 
    AND DATE_TRUNC('month', p.creation_date) :: date = '2008-12-01'
               )
SELECT COUNT(t.id),
       DATE_TRUNC('month', pos.creation_date) :: date 
FROM t
JOIN stackoverflow.posts AS pos ON pos.user_id = t.id
GROUP BY DATE_TRUNC('month', pos.creation_date)  

ORDER BY date_trunc DESC     

---

WITH t AS (
    SELECT COUNT(DISTINCT user_id),
    user_id,
           CAST(DATE_TRUNC('day', creation_date) as date)
    FROM stackoverflow.posts
    WHERE  CAST(DATE_TRUNC('day', creation_date) as date) BETWEEN '2008-12-01' AND '2008-12-07'
    GROUP BY user_id, CAST(DATE_TRUNC('day', creation_date) as date)
    ORDER BY user_id, CAST(DATE_TRUNC('day', creation_date) as date)),
   
s as (SELECT SUM(count), user_id  /* ROUND(AVG(count)) */
FROM t
GROUP BY user_id)
SELECT ROUND(AVG(sum))
FROM s

---

WITH ts AS (
    SELECT *, 
           EXTRACT(WEEK FROM CAST(creation_date AS date))      
    FROM (
        SELECT  user_id, 
               COUNT (user_id)    
        FROM stackoverflow.posts
        GROUP BY user_id
        ORDER BY count DESC
        LIMIT 1) as t
LEFT JOIN stackoverflow.posts  AS p ON p.user_id = t.user_id
WHERE CAST(DATE_TRUNC('month', creation_date) AS date) = '2008-10-01')
SELECT DISTINCT 
       date_part,
       LAST_VALUE(creation_date) OVER (PARTITION BY date_part ORDER BY creation_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM ts


