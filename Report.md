датасет: https://www.kaggle.com/datasets/govzegunns/mimovrste-2021-2023-item-prices
размер: 94gb

# Запросы в hive:
1. ```sql
   select name from ItemPrice where current_price > 500 
   ```
    ![Screenshot 2024-04-07 at 11.52.45.png](images%2FScreenshot%202024-04-07%20at%2011.52.45.png)
    время: 4.07
2. ```sql
   select COUNT(name) from ItemPrice where current_price > 500;
   ```
    ![Screenshot 2024-04-07 at 12.05.00.png](images%2FScreenshot%202024-04-07%20at%2012.05.00.png)
    время: 4.43

3.
    ```sql
    SELECT brand_name, AVG(review_stars) AS avg_review_stars
    FROM ItemPrice
    GROUP BY brand_name
    ORDER BY avg_review_stars DESC 
    LIMIT 25;
    ```
    ![Screenshot 2024-04-07 at 12.17.46.png](images%2FScreenshot%202024-04-07%20at%2012.17.46.png)
    ![Screenshot 2024-04-07 at 12.18.16.png](images%2FScreenshot%202024-04-07%20at%2012.18.16.png)
    время: 9.11 + 0.18 = 9.29 
4. ```sql
   WITH category_avg_price AS (
    SELECT category_name, AVG(current_price) AS avg_price
    FROM ItemPrice
    GROUP BY category_name
    )
    SELECT category_name, avg_price
    FROM category_avg_price
    ORDER BY avg_price DESC
    LIMIT 10;
    ```
   ![Screenshot 2024-04-07 at 12.41.00.png](images%2FScreenshot%202024-04-07%20at%2012.41.00.png)
   ![Screenshot 2024-04-07 at 12.42.13.png](images%2FScreenshot%202024-04-07%20at%2012.42.13.png)
   время: 9.36 + 0.19 = 9.55

# Запросы в спарк используя df:
Query 1 Execution Time: 0.2736837863922119 seconds время маленькое из-за линивых вычислений, происходит посик первых 20 выполнений условия, далее вычисления не происходят
Query 2 Execution Time: 41.756088733673096 seconds
Query 3 Execution Time: 45.17482781410217 seconds
Query 4 Execution Time: 64.34221243858337 seconds

# Запросы в спарк используя ds:
Query 1 Execution Time: 1.3839216232299805 seconds время маленькое из-за линивых вычислений, происходит посик первых 20 выполнений условия, далее вычисления не происходят
Query 2 Execution Time: 58.35791802406311 seconds
Query 3 Execution Time: 43.32658076286316 seconds
Query 4 Execution Time: 62.49212718009949 seconds

# Запросы используя rdd

Query 1 Execution Time: 62.96712374687195 seconds                               
Query 2 Execution Time: 62.49861454963684 seconds
Query 3 Execution Time: 66.35412836074829 seconds                               
Query 4 Execution Time: 73.69891786575317 seconds   

# топ 5 символов в дс используя rdd
[('e', 5200742704), ('o', 5028270385), ('a', 4966077532), ('i', 4744810189), ('t', 4186305067)]

# использование библиотеки в спарк
Была жуткая необходимость красивого вывода в логи, поэтому была добавлина библионтека art
![Screenshot 2024-04-07 at 20.13.13.png](images%2FScreenshot%202024-04-07%20at%2020.13.13.png)

ссылка на архив: https://dropmefiles.com/3HNl0

