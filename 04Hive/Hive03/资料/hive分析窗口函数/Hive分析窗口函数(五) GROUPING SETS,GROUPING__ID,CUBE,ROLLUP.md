### Hive分析窗口函数(五) GROUPING SETS,GROUPING__ID,CUBE,ROLLUP

​	这几个分析函数通常用于OLAP中，不能累加，而且需要根据不同维度上钻和下钻的指标统计，比如，分小时、天、月的UV数。

- 数据准备

  ```
  2018-03,2018-03-10,cookie1
  2018-03,2018-03-10,cookie5
  2018-03,2018-03-12,cookie7
  2018-04,2018-04-12,cookie3
  2018-04,2018-04-13,cookie2
  2018-04,2018-04-13,cookie4
  2018-04,2018-04-16,cookie4
  2018-03,2018-03-10,cookie2
  2018-03,2018-03-10,cookie3
  2018-04,2018-04-12,cookie5
  2018-04,2018-04-13,cookie6
  2018-04,2018-04-15,cookie3
  2018-04,2018-04-15,cookie2
  2018-04,2018-04-16,cookie1
   
  CREATE TABLE itcast_t5 (
  month STRING,
  day STRING, 
  cookieid STRING 
  ) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
  stored as textfile;
  
  加载数据：
  load data local inpath '/root/hivedata/itcast_t5.dat' into table itcast_t5;
  ```

----

- GROUPING SETS

  grouping sets是一种将多个group by 逻辑写在一个sql语句中的便利写法。

  等价于将不同维度的GROUP BY结果集进行UNION ALL。

  **GROUPING__ID**，表示结果属于哪一个分组集合。

  ```
  SELECT 
  month,
  day,
  COUNT(DISTINCT cookieid) AS uv,
  GROUPING__ID 
  FROM itcast_t5 
  GROUP BY month,day 
  GROUPING SETS (month,day) 
  ORDER BY GROUPING__ID;
  
  grouping_id表示这一组结果属于哪个分组集合，
  根据grouping sets中的分组条件month，day，1是代表month，2是代表day
  
  等价于 
  SELECT month,NULL,COUNT(DISTINCT cookieid) AS uv,1 AS GROUPING__ID FROM itcast_t5 GROUP BY month UNION ALL 
  SELECT NULL as month,day,COUNT(DISTINCT cookieid) AS uv,2 AS GROUPING__ID FROM itcast_t5 GROUP BY day;
  ```

  再如：

  ```
  SELECT 
  month,
  day,
  COUNT(DISTINCT cookieid) AS uv,
  GROUPING__ID 
  FROM itcast_t5 
  GROUP BY month,day 
  GROUPING SETS (month,day,(month,day)) 
  ORDER BY GROUPING__ID;
  
  等价于
  SELECT month,NULL,COUNT(DISTINCT cookieid) AS uv,1 AS GROUPING__ID FROM itcast_t5 GROUP BY month 
  UNION ALL 
  SELECT NULL,day,COUNT(DISTINCT cookieid) AS uv,2 AS GROUPING__ID FROM itcast_t5 GROUP BY day
  UNION ALL 
  SELECT month,day,COUNT(DISTINCT cookieid) AS uv,3 AS GROUPING__ID FROM itcast_t5 GROUP BY month,day;
  ```

- CUBE

  根据GROUP BY的维度的所有组合进行聚合。

  ```
  SELECT 
  month,
  day,
  COUNT(DISTINCT cookieid) AS uv,
  GROUPING__ID 
  FROM itcast_t5 
  GROUP BY month,day 
  WITH CUBE 
  ORDER BY GROUPING__ID;
  
  等价于
  SELECT NULL,NULL,COUNT(DISTINCT cookieid) AS uv,0 AS GROUPING__ID FROM itcast_t5
  UNION ALL 
  SELECT month,NULL,COUNT(DISTINCT cookieid) AS uv,1 AS GROUPING__ID FROM itcast_t5 GROUP BY month 
  UNION ALL 
  SELECT NULL,day,COUNT(DISTINCT cookieid) AS uv,2 AS GROUPING__ID FROM itcast_t5 GROUP BY day
  UNION ALL 
  SELECT month,day,COUNT(DISTINCT cookieid) AS uv,3 AS GROUPING__ID FROM itcast_t5 GROUP BY month,day;
  ```

- ROLLUP

  是CUBE的子集，以最左侧的维度为主，从该维度进行层级聚合。

  ```
  比如，以month维度进行层级聚合：
  SELECT 
  month,
  day,
  COUNT(DISTINCT cookieid) AS uv,
  GROUPING__ID  
  FROM itcast_t5 
  GROUP BY month,day
  WITH ROLLUP 
  ORDER BY GROUPING__ID;
  
  --把month和day调换顺序，则以day维度进行层级聚合：
   
  SELECT 
  day,
  month,
  COUNT(DISTINCT cookieid) AS uv,
  GROUPING__ID  
  FROM itcast_t5 
  GROUP BY day,month 
  WITH ROLLUP 
  ORDER BY GROUPING__ID;
  （这里，根据天和月进行聚合，和根据天聚合结果一样，因为有父子关系，如果是其他维度组合的话，就会不一样）
  ```
























