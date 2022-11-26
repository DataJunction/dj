SELECT CASE
         WHEN web.item_sk IS NULL THEN web.item_sk
         ELSE store.item_sk
       END              item_sk,
       CASE
         WHEN web.d_date IS NOT NULL THEN web.d_date
         ELSE store.d_date
       END              d_date,
       web.cume_sales   web_sales,
       store.cume_sales store_sales
FROM   web_v1 web 
