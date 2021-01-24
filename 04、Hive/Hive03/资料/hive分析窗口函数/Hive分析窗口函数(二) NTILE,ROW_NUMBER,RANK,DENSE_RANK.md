### Hive分析窗口函数(二) NTILE,ROW_NUMBER,RANK,DENSE_RANK

- 数据准备

  ```
  cookie1,2018-04-10,1
  cookie1,2018-04-11,5
  cookie1,2018-04-12,7
  cookie1,2018-04-13,3
  cookie1,2018-04-14,2
  cookie1,2018-04-15,4
  cookie1,2018-04-16,4
  cookie2,2018-04-10,2
  cookie2,2018-04-11,3
  cookie2,2018-04-12,5
  cookie2,2018-04-13,6
  cookie2,2018-04-14,3
  cookie2,2018-04-15,9
  cookie2,2018-04-16,7
   
  CREATE TABLE itcast_t2 (
  cookieid string,
  createtime string,   --day 
  pv INT
  ) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
  stored as textfile;
    
  加载数据：
  load data local inpath '/root/hivedata/itcast_t2.dat' into table itcast_t2;
  ```

---

- NTILE

  背景：

  ​	有时会有这样的需求:如果数据排序后分为三部分，业务人员只关心其中的一部分，如何将这中间的三分之一数据拿出来呢?NTILE函数即可以满足。

  ```
  ntile可以看成是：把有序的数据集合平均分配到指定的数量（num）个桶中, 将桶号分配给每一行。
  如果不能平均分配，则优先分配较小编号的桶，并且各个桶中能放的行数最多相差1。
  语法是：ntile (num)  over ([partition_clause]  order_by_clause)  as xxx
  然后可以根据桶号，选取前或后 n分之几的数据。
  数据会完整展示出来，只是给相应的数据打标签；具体要取几分之几的数据，需要再嵌套一层根据标签取出。
  NTILE不支持ROWS BETWEEN，比如 NTILE(2) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
  ```

  ```
  SELECT 
  cookieid,
  createtime,
  pv,
  NTILE(2) OVER(PARTITION BY cookieid ORDER BY createtime) AS rn1,
  NTILE(3) OVER(PARTITION BY cookieid ORDER BY createtime) AS rn2,
  NTILE(4) OVER(ORDER BY createtime) AS rn3
  FROM itcast_t2 
  ORDER BY cookieid,createtime;
  ```

  比如，统计一个cookie，pv数最多的前1/3的天

  ```
  SELECT 
  cookieid,
  createtime,
  pv,
  NTILE(3) OVER(PARTITION BY cookieid ORDER BY pv DESC) AS rn 
  FROM itcast_t2;
   
  其中rn = 1 的记录，就是我们想要的结果
  ```

---

- ROW_NUMBER

  ROW_NUMBER()  从1开始，按照顺序，生成分组内记录的序列

  ```
  SELECT 
  cookieid,
  createtime,
  pv,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY pv desc) AS rn 
  FROM itcast_t2;
  ```

- RANK 和 DENSE_RANK

  RANK() 生成数据项在分组中的排名，排名相等会在名次中留下空位
  DENSE_RANK() 生成数据项在分组中的排名，排名相等会在名次中不会留下空位

  ```
  SELECT 
  cookieid,
  createtime,
  pv,
  RANK() OVER(PARTITION BY cookieid ORDER BY pv desc) AS rn1,
  DENSE_RANK() OVER(PARTITION BY cookieid ORDER BY pv desc) AS rn2,
  ROW_NUMBER() OVER(PARTITION BY cookieid ORDER BY pv DESC) AS rn3 
  FROM itcast_t2 
  WHERE cookieid = 'cookie1';
  ```
