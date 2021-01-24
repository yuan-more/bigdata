### Hive分析窗口函数(四) LAG,LEAD,FIRST_VALUE,LAST_VALUE

**注意： 这几个函数不支持WINDOW子句**

- 准备数据

  ```
  cookie1,2018-04-10 10:00:02,url2
  cookie1,2018-04-10 10:00:00,url1
  cookie1,2018-04-10 10:03:04,1url3
  cookie1,2018-04-10 10:50:05,url6
  cookie1,2018-04-10 11:00:00,url7
  cookie1,2018-04-10 10:10:00,url4
  cookie1,2018-04-10 10:50:01,url5
  cookie2,2018-04-10 10:00:02,url22
  cookie2,2018-04-10 10:00:00,url11
  cookie2,2018-04-10 10:03:04,1url33
  cookie2,2018-04-10 10:50:05,url66
  cookie2,2018-04-10 11:00:00,url77
  cookie2,2018-04-10 10:10:00,url44
  cookie2,2018-04-10 10:50:01,url55
   
  CREATE TABLE itcast_t4 (
  cookieid string,
  createtime string,  --页面访问时间
  url STRING       --被访问页面
  ) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
  stored as textfile;
  
  加载数据：
  load data local inpath '/root/hivedata/itcast_t4.dat' into table itcast_t4;
  ```

---

- LAG

  **LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值**
  第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）

  ```
  SELECT cookieid,
  createtime,
  url,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rn,
  LAG(createtime,1,'1970-01-01 00:00:00') OVER(PARTITION BY cookieid ORDER BY createtime) AS last_1_time,
  LAG(createtime,2) OVER(PARTITION BY cookieid ORDER BY createtime) AS last_2_time 
  FROM itcast_t4;
  
  
  last_1_time: 指定了往上第1行的值，default为'1970-01-01 00:00:00'  
               			 cookie1第一行，往上1行为NULL,因此取默认值 1970-01-01 00:00:00
               			 cookie1第三行，往上1行值为第二行值，2015-04-10 10:00:02
               			 cookie1第六行，往上1行值为第五行值，2015-04-10 10:50:01
  last_2_time: 指定了往上第2行的值，为指定默认值
  						 cookie1第一行，往上2行为NULL
  						 cookie1第二行，往上2行为NULL
  						 cookie1第四行，往上2行为第二行值，2015-04-10 10:00:02
  						 cookie1第七行，往上2行为第五行值，2015-04-10 10:50:01
  ```

- LEAD

  与LAG相反
  **LEAD(col,n,DEFAULT) 用于统计窗口内往下第n行值**
  第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）

  ```
  SELECT cookieid,
  createtime,
  url,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rn,
  LEAD(createtime,1,'1970-01-01 00:00:00') OVER(PARTITION BY cookieid ORDER BY createtime) AS next_1_time,
  LEAD(createtime,2) OVER(PARTITION BY cookieid ORDER BY createtime) AS next_2_time 
  FROM itcast_t4;
  ```

- FIRST_VALUE

  取分组内排序后，截止到当前行，第一个值

  ```
  SELECT cookieid,
  createtime,
  url,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rn,
  FIRST_VALUE(url) OVER(PARTITION BY cookieid ORDER BY createtime) AS first1 
  FROM itcast_t4;
  ```

- LAST_VALUE

  取分组内排序后，截止到当前行，最后一个值

  ```
  SELECT cookieid,
  createtime,
  url,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rn,
  LAST_VALUE(url) OVER(PARTITION BY cookieid ORDER BY createtime) AS last1 
  FROM itcast_t4;
  ```

  如果想要取分组内排序后最后一个值，则需要变通一下：

  ```
  SELECT cookieid,
  createtime,
  url,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY createtime) AS rn,
  LAST_VALUE(url) OVER(PARTITION BY cookieid ORDER BY createtime) AS last1,
  FIRST_VALUE(url) OVER(PARTITION BY cookieid ORDER BY createtime DESC) AS last2 
  FROM itcast_t4 
  ORDER BY cookieid,createtime;
  ```



  **特别注意order  by**

  如果不指定ORDER BY，则进行排序混乱，会出现错误的结果

  ```
  SELECT cookieid,
  createtime,
  url,
  FIRST_VALUE(url) OVER(PARTITION BY cookieid) AS first2  
  FROM itcast_t4;
  ```






















