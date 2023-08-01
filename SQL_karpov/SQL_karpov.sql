-- Пример решения задач SQL

--- АНАЛИЗ ПРОДУКТОВЫХ МЕТРИК
with f5 as (SELECT date as dd,
                   sum(revenue) OVER (ORDER BY date) as total_revenue,
                   *
            FROM   (SELECT creation_time::date as date,
                           sum(price) as revenue
                    FROM   (SELECT order_id,
                                   creation_time,
                                   unnest(product_ids) as product_id
                            FROM   orders
                            WHERE  order_id not in (SELECT order_id
                                                    FROM   user_actions
                                                    WHERE  action = 'cancel_order')) t1
                        LEFT JOIN products using(product_id)
                    GROUP BY date) as t2 full join (SELECT date,
                                                   sum(count) OVER(ORDER BY date) as all_users
                                            FROM   (SELECT date,
                                                           count(user_id)
                                                    FROM   (SELECT user_id,
                                                                   min(time) :: date as date
                                                            FROM   user_actions
                                                            GROUP BY user_id) as t3
                                                    GROUP BY date
                                                    ORDER BY 1) as t4) as f1 using(date) full join (SELECT date,
                                                                   sum(count) OVER(ORDER BY date) as revenue_users
                                                            FROM   (SELECT date,
                                                                           count(user_id)
                                                                    FROM   (SELECT user_id,
                                                                                   min(time) :: date as date
                                                                            FROM   user_actions
                                                                            WHERE  order_id not in (SELECT order_id
                                                                                                    FROM   user_actions
                                                                                                    WHERE  action = 'cancel_order')
                                                                            GROUP BY user_id) as t3
                                                                    GROUP BY date
                                                                    ORDER BY 1) as t4) as f2 using(date) full join (SELECT date,
                                                                   sum(count) OVER (ORDER BY date) as total_orders
                                                            FROM   (SELECT creation_time :: date as date,
                                                                           count(order_id)
                                                                    FROM   orders
                                                                    WHERE  order_id not in (SELECT order_id
                                                                                            FROM   user_actions
                                                                                            WHERE  action = 'cancel_order')
                                                                    GROUP BY date) as t5) as f3 using(date))
SELECT date,
       round(total_revenue / all_users * 1.0, 2) as running_arpu,
       round(total_revenue / revenue_users * 1.0, 2) as running_arppu,
       round(total_revenue / total_orders * 1.0, 2) as running_aov
FROM   f5

---- 
  
with t2 as (SELECT creation_time :: date as date,
                   sum(price) as daily_revenue
            FROM   (SELECT order_id,
                           creation_time,
                           unnest(product_ids) as product_id
                    FROM   orders
                    WHERE  order_id not in (SELECT order_id
                                            FROM   user_actions
                                            WHERE  action = 'cancel_order')) as t
                LEFT JOIN products using(product_id)
            GROUP BY date)
SELECT date,
       daily_revenue,
       coalesce(daily_revenue - lag(daily_revenue, 1) OVER(ORDER BY date),
                0) as revenue_growth_abs,
       coalesce(round((daily_revenue - lag(daily_revenue, 1) OVER(ORDER BY date)) / (lead(daily_revenue, -1) OVER(ORDER BY date)) * 100, 1),
                0) as revenue_growth_percentage
FROM   t2
  
----
  
SELECT date,
       revenue,
       sum(revenue) OVER (ORDER BY date) as total_revenue,
       round(100 * (revenue * 1.0 - lag(revenue) OVER (ORDER BY date)) / lag(revenue) OVER (ORDER BY date),
             2) as revenue_change
FROM   (SELECT date,
               sum(price) as revenue FROM(SELECT creation_time :: date as date,
                                          unnest(array[product_ids]) as product_id
                                   FROM   orders
                                   WHERE  order_id not in (SELECT order_id
                                                           FROM   user_actions
                                                           WHERE  action = 'cancel_order')) as t1
            LEFT JOIN products using(product_id)
        GROUP BY date) as t2
ORDER BY date

----
SELECT date,
       orders,
       first_orders,
       new_users_orders :: int,
       round(first_orders / orders :: numeric * 100, 2) as first_orders_share,
       round(new_users_orders / orders :: numeric * 100,
                                                                                           2) as new_users_orders_share FROM(SELECT time :: date as date,
                                         count(user_id) as orders
                                  FROM   user_actions
                                  WHERE  order_id not in (SELECT order_id
                                                          FROM   user_actions
                                                          WHERE  action = 'cancel_order')
                                  GROUP BY date) as t4
    LEFT JOIN (SELECT date_min,
                      count(user_id) as first_orders
               FROM   (SELECT min(time :: date) as date_min,
                              user_id
                       FROM   user_actions
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY user_id) as t1
               GROUP BY date_min) t5
        ON t4.date = t5.date_min
    LEFT JOIN (SELECT first_date,
                      sum(coalesce(count, 0)) as new_users_orders
               FROM   (SELECT min(time::date) as first_date,
                              user_id
                       FROM   user_actions
                       GROUP BY user_id) t2
                   LEFT JOIN (SELECT time :: date as date1,
                                     user_id,
                                     count(order_id)
                              FROM   user_actions
                              WHERE  order_id not in (SELECT order_id
                                                      FROM   user_actions
                                                      WHERE  action = 'cancel_order')
                              GROUP BY user_id, date1) as t3
                       ON t2.user_id = t3.user_id and
                          t2.first_date = t3.date1
               GROUP BY first_date) t6
        ON t4.date = t6.first_date
ORDER BY date

----

with user_t as(SELECT user_id,
                      date_trunc('day', min(time)) as date
               FROM   user_actions
               GROUP BY user_id
               ORDER BY date), t as (SELECT count(user_id) as new_users,
                             date
                      FROM   user_t
                      GROUP BY date), courier_t as(SELECT courier_id,
                                    date_trunc('day', min(time)) as date
                             FROM   courier_actions
                             GROUP BY courier_id
                             ORDER BY date), t2 as (SELECT count(courier_id) as new_couriers,
                              date as date1
                       FROM   courier_t
                       GROUP BY date), t3 as (SELECT t.date ::date ,
                              new_users,
                              new_couriers,
                              cast(sum(new_users) OVER(ORDER BY t.date) as int) as total_users,
                              cast(sum(new_couriers) OVER(ORDER BY date) as int) as total_couriers,
                              round((new_users::numeric - lag(new_users) OVER (ORDER BY t.date)) / (lag(new_users) OVER (ORDER BY t.date)) * 100,
                                    2) as new_users_change,
                              round((new_couriers::numeric - lag(new_couriers) OVER (ORDER BY t.date)) / (lag(new_couriers) OVER (ORDER BY t.date)) * 100,
                                    2) as new_couriers_change
                       FROM   t full join t2
                               ON t.date = t2.date1)
SELECT date,
       new_users,
       new_couriers,
       total_users,
       total_couriers,
       new_users_change,
       new_couriers_change,
       round((total_users::numeric - lag(total_users) OVER (ORDER BY date)) / lag(total_users) OVER (ORDER BY date) * 100,
             2) as total_users_growth,
       round((total_couriers::numeric - lag(total_couriers) OVER (ORDER BY date)) / lag(total_couriers) OVER (ORDER BY date) * 100,
             2) as total_couriers_growth
FROM   t3
  
----   ОКОННЫЕ ФУНКЦИИ 
  
with t2 as (SELECT date,
                   sum(price) as revenue FROM(SELECT creation_time :: date as date,
                                              unnest(array[product_ids]) as product_id
                                       FROM   orders
                                       WHERE  order_id not in (SELECT order_id
                                                               FROM   user_actions
                                                               WHERE  action = 'cancel_order')) as t1
                LEFT JOIN products using(product_id)
            GROUP BY date), t3 as (SELECT time :: date as date,
                              count(distinct user_id) as all_order
                       FROM   user_actions
                       GROUP BY date), t4 as (SELECT time :: date as date,
                              count(distinct user_id) as good_order
                       FROM   user_actions
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY date), t5 as (SELECT creation_time :: date as date,
                              count(distinct order_id) as ord
                       FROM   orders
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY date)
SELECT date,
       round(revenue :: numeric / all_order, 2) as arpu,
       round(revenue :: numeric / good_order, 2) as arppu,
       round(revenue :: numeric / ord, 2) as aov
FROM   t2 full join t3 using(date) full join t4 using(date) full join t5 using(date)

  
----
with t2 as (SELECT order_id,
                   creation_time,
                   sum(price) as order_price,
                   date_trunc('day', creation_time) as data
            FROM   (SELECT order_id,
                           creation_time,
                           unnest(product_ids) as product_id
                    FROM   orders
                    WHERE  order_id not in (SELECT order_id
                                            FROM   user_actions
                                            WHERE  action = 'cancel_order')) as t
                LEFT JOIN products using(product_id)
            GROUP BY order_id, creation_time)
SELECT order_id,
       creation_time,
       order_price,
       sum(order_price) OVER(PARTITION BY data) as daily_revenue,
       round(order_price / sum(order_price) OVER(PARTITION BY data) * 100,
             3) as percentage_of_daily_revenue
FROM   t2
ORDER BY data desc, percentage_of_daily_revenue desc, order_id

---

with t as (SELECT *,
                  count(order_id) filter (WHERE action = 'deliver_order') OVER (PARTITION BY courier_id
                                                                                ORDER BY order_id) as delivered_orders,
                  age(max(time :: date) OVER(),
                      min(time :: date) filter(WHERE action = 'accept_order') OVER(PARTITION BY courier_id)) as days
           FROM   courier_actions)
SELECT courier_id,
       date_part('day', days) as days_employed,
       max(delivered_orders) as delivered_orders
FROM   t
WHERE  date_part('day', days) >= 10
GROUP BY courier_id, days
ORDER BY days_employed desc, courier_id

---

with s as (SELECT *,
                  row_number() OVER(ORDER BY orders_count desc, courier_id) as courier_rank
           FROM   (SELECT courier_id,
                          count(order_id) as orders_count
                   FROM   courier_actions
                   WHERE  action = 'deliver_order'
                   GROUP BY courier_id
                   ORDER BY orders_count desc, courier_id) as t)
SELECT *
FROM   s
WHERE  courier_rank <= (SELECT round(count(distinct courier_id) * 0.1)
                        FROM   courier_actions)
                        
---

SELECT *,
       count(order_id) filter(WHERE action = 'create_order') OVER(PARTITION BY user_id
                                                                  ORDER BY time) as created_orders,
       count(order_id) filter(WHERE action != 'create_order') OVER(PARTITION BY user_id
                                                                   ORDER BY time) as canceled_orders,
       round(count(order_id) filter(WHERE action != 'create_order') OVER(PARTITION BY user_id
                                                                         ORDER BY time) * 1.0/ count(order_id) filter(WHERE action = 'create_order') OVER(PARTITION BY user_id ORDER BY time), 2) as cancel_rate
FROM   user_actions
ORDER BY user_id, order_id, time limit 1000

--- 

with t as(SELECT user_id,
                 order_id,
                 time,
                 row_number() OVER(PARTITION BY user_id
                                   ORDER BY time) as order_number,
                 extract(epoch
          FROM   (time - (lag(time, 1)
          OVER(
          PARTITION BY user_id
          ORDER BY time)))) / 3600 as time_diff
          FROM   user_actions
          WHERE  order_id not in (SELECT order_id
                                  FROM   user_actions
                                  WHERE  action = 'cancel_order')
          ORDER BY user_id, order_number)
SELECT user_id,
       round(avg(time_diff)) as hours_between_orders
FROM   t
GROUP BY user_id having count(user_id) > 1
ORDER BY user_id limit 1000

--- Объединяем таблицы

with query as (SELECT order_id,
                      name
               FROM   (SELECT order_id,
                              unnest(product_ids) as product_id
                       FROM   orders
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')) subquery_1 join products using(product_id))
SELECT pair,
       count(pair) as count_pair
FROM   (SELECT DISTINCT order_id,
                        string_to_array(one || '.' || two, '.') as pair
        FROM   (SELECT order_id,
                       t1.name as one,
                       t2.name as two
                FROM   query t1 join query t2 using(order_id)
                WHERE  t1.name < t2.name) subquery_2) subquery_3
GROUP BY pair
ORDER BY count_pair desc, pair

--- 

with order_id_large_size as (SELECT order_id
                             FROM   orders
                             WHERE  array_length(product_ids, 1) = (SELECT max(array_length(product_ids, 1))
                                                                    FROM   orders))
SELECT DISTINCT order_id,
                user_id,
                date_part('year', age((SELECT max(time)
                       FROM   user_actions), users.birth_date)) as user_age, courier_id, date_part('year', age((SELECT max(time)
                                                                                         FROM   user_actions), couriers.birth_date)) as courier_age
FROM   (SELECT order_id,
               user_id
        FROM   user_actions
        WHERE  order_id in (SELECT *
                            FROM   order_id_large_size)) t1
    LEFT JOIN (SELECT order_id,
                      courier_id
               FROM   courier_actions
               WHERE  order_id in (SELECT *
                                   FROM   order_id_large_size)) t2 using(order_id)
    LEFT JOIN users using(user_id)
    LEFT JOIN couriers using(courier_id)
ORDER BY order_id

---

SELECT order_id,
       array_agg(name) as product_names
FROM   (SELECT order_id,
               unnest(product_ids) as product_id
        FROM   orders) as t
    LEFT JOIN products using(product_id)
GROUP BY order_id limit 1000

---

SELECT order_id
FROM   courier_actions
WHERE  order_id in (SELECT DISTINCT (order_id)
                    FROM   courier_actions
                    WHERE  action = 'deliver_order')
GROUP BY order_id
ORDER BY max(time) - min(time) desc limit 10

---

with t as (SELECT user_id,
                  round(count(distinct order_id) filter(WHERE action = 'cancel_order') / (count(distinct order_id) * 1.0),
                        5) as cancel_rate
           FROM   user_actions
           GROUP BY user_id
           ORDER BY cancel_rate desc), d as (SELECT user_id,
                                         coalesce(sex, 'unknown') as sex,
                                         cancel_rate
                                  FROM   t
                                      LEFT JOIN users using(user_id))
SELECT sex,
       round(avg(cancel_rate), 3) as avg_cancel_rate
FROM   d
GROUP BY sex
ORDER BY sex

---

SELECT name,
       count(product_id) as times_purchased
FROM   (SELECT DISTINCT order_id,
                        unnest(array[product_ids]) as product_id
        FROM   orders
            LEFT JOIN courier_actions using(order_id)
        WHERE  time between '2022-09-01'
           and '2022-09-30'
           and order_id in (SELECT order_id
                         FROM   courier_actions
                         WHERE  action = 'deliver_order')) as t1
    LEFT JOIN products using(product_id)
GROUP BY name
ORDER BY times_purchased desc limit 10

---

with a as(SELECT user_id,
                 order_id
          FROM   user_actions
          WHERE  order_id not in (SELECT order_id
                                  FROM   user_actions
                                  WHERE  action = 'cancel_order')), b as(SELECT order_id,
                                              sum(price) as order_price
                                       FROM   (SELECT order_id,
                                                      unnest(product_ids) as product_id
                                               FROM   orders)query_in join products using(product_id)
                                       GROUP BY 1)
SELECT user_id,
       count(order_id) as orders_count,
       round(avg(array_length(product_ids, 1)), 2) as avg_order_size,
       sum(order_price) as sum_order_value,
       round(avg(order_price), 2) as avg_order_value,
       min(order_price) as min_order_value ,
       max(order_price) as max_order_value
FROM   a
    LEFT JOIN b using(order_id)
    LEFT JOIN orders using(order_id)
GROUP BY user_id
ORDER BY 1 limit 1000

---

SELECT user_id,
       order_id,
       product_ids
FROM   (SELECT order_id,
               user_id
        FROM   user_actions
        WHERE  order_id not in (SELECT order_id
                                FROM   user_actions
                                WHERE  action = 'cancel_order')) as a
    LEFT JOIN orders using(order_id)
ORDER BY user_id, order_id limit 1000

---

SELECT user_id,
       name
FROM   (SELECT user_id
        FROM   users limit 100) as a, (SELECT name
                               FROM   products) as m
ORDER BY user_id, name

--- ПОДЗАПРОСЫ

with t as (SELECT user_id,
                  (age((SELECT max(time)
                 FROM   user_actions)::date, birth_date))
           FROM   users) , test as (SELECT user_id ,
                                extract(year
                         FROM   age) as data
                         FROM   t
                         ORDER BY user_id)
SELECT user_id,
       case when data isnull then (SELECT round(avg(data))
                            FROM   test) else data end as age
FROM   test
GROUP BY user_id, data
ORDER BY user_id

---

with t as (SELECT * ,
                  unnest(product_ids) as product_i
           FROM   orders)
SELECT DISTINCT(order_id),
                product_ids
FROM   t
WHERE  product_i in (SELECT product_id
                     FROM   products
                     ORDER BY price desc limit 5)
ORDER BY order_id

---

SELECT unnest(product_ids) as product_id,
       count(order_id) as times_purchased
FROM   orders
GROUP BY product_id
ORDER BY times_purchased desc limit 10

---

SELECT creation_time,
       order_id,
       product_ids,
       unnest(array[product_ids]) as product_id
FROM   orders limit 100

--- ГРУППИРОВКА ДАННЫХ

SELECT (case when date_part('year', age(birth_date)) between 19 and
                  24 then '19-24'
             when date_part('year', age(birth_date)) between 25 and
                  29 then '25-29'
             when date_part('year', age(birth_date)) between 30 and
                  35 then '30-35'
             when date_part('year', age(birth_date)) between 36 and
                  41 then '36-41' end) as group_age,
       count(birth_date) as users_count
FROM   users
WHERE  birth_date is not null
GROUP BY group_age
ORDER BY group_age

---

SELECT user_id,
       round(count(distinct order_id) filter(WHERE action = 'cancel_order') / (count(distinct order_id) * 1.0),
             2) as cancel_rate
FROM   user_actions
GROUP BY user_id
ORDER BY user_id

---

SELECT courier_id,
       count(action) as delivered_orders
FROM   courier_actions
WHERE  action = 'deliver_order'
   and date_part('year', time) = 2022
   and date_part('month', time) = 09
GROUP BY courier_id having count(action) = 1
ORDER BY courier_id

--- АГРЕГАЦИЯ ДАННЫХ

SELECT count(order_id) as orders,
       count(order_id) filter(WHERE(array_length(product_ids, 1) >= 5)) as large_orders,
       round(count(order_id) filter(WHERE(array_length(product_ids, 1) >= 5)) * 1.0 / count(order_id),
             2) as large_orders_share
FROM   orders

---

SELECT count(distinct user_id) - count(distinct user_id) filter(WHERE action = 'cancel_order') as users_count
FROM   user_actions

---

SELECT count(distinct user_id) as unique_users,
       count(distinct order_id) as unique_orders,
       round(count(distinct order_id) / (0.1 + count(distinct user_id) - 0.1),
             2) as orders_per_user
FROM   user_actions

--- ФИЛЬТРАЦИЯ ДАННЫХ

SELECT product_id,
       name,
       price,
       case when name in ('сахар', 'сухарики', 'сушки', 'семечки', 'масло льняное', 'виноград', 'масло оливковое', 'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 'овсянка', 'макароны', 'баранина', 'апельсины', 'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 'мука', 'шпроты', 'сосиски', 'свинина', 'рис', 'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') then round(price / 110 * 10,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         2)
            else round(price / 120 * 20 , 2) end as tax ,
       case when name in ('сахар', 'сухарики', 'сушки', 'семечки', 'масло льняное', 'виноград', 'масло оливковое', 'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 'овсянка', 'макароны', 'баранина', 'апельсины', 'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 'мука', 'шпроты', 'сосиски', 'свинина', 'рис', 'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') then round(price - price/110*10,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         2)
            else round(price - price/120*20, 2) end as price_before_tax
FROM   products
ORDER BY price_before_tax desc, product_id

---

SELECT user_id,
       order_id,
       action,
       time
FROM   user_actions
WHERE  action = 'cancel_order'
   and date_part('year', time) = 2022
   and date_part('month', time) = 08
   and date_part('dow', time) = 3
   and date_part('hour', time) between 12
   and 15
ORDER BY time desc

---

