-- Пример решения задач SQL

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
