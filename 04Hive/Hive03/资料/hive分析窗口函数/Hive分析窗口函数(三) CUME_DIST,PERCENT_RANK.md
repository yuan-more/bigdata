### Hive分析窗口函数(三) CUME_DIST,PERCENT_RANK

这两个序列分析函数不是很常用，**注意： 序列函数不支持WINDOW子句**

- 数据准备

  ```
  d1,user1,1000
  d1,user2,2000
  d1,user3,3000
  d2,user4,4000
  d2,user5,5000
   
  CREATE EXTERNAL TABLE itcast_t3 (
  dept STRING,
  userid string,
  sal INT
  ) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
  stored as textfile;
  
  加载数据：
  load data local inpath '/root/hivedata/itcast_t3.dat' into table itcast_t3;
  ```

---

- CUME_DIST  和order byd的排序顺序有关系

  CUME_DIST 小于等于当前值的行数/分组内总行数  order 默认顺序 正序 升序
  比如，统计小于等于当前薪水的人数，所占总人数的比例

  ```
  SELECT 
  dept,
  userid,
  sal,
  CUME_DIST() OVER(ORDER BY sal) AS rn1,
  CUME_DIST() OVER(PARTITION BY dept ORDER BY sal) AS rn2 
  FROM itcast_t3;
  
  rn1: 没有partition,所有数据均为1组，总行数为5，
       第一行：小于等于1000的行数为1，因此，1/5=0.2
       第三行：小于等于3000的行数为3，因此，3/5=0.6
  rn2: 按照部门分组，dpet=d1的行数为3,
       第二行：小于等于2000的行数为2，因此，2/3=0.6666666666666666
  ```

- PERCENT_RANK

  PERCENT_RANK 分组内当前行的RANK值-1/分组内总行数-1

  经调研 该函数显示现实意义不明朗 有待于继续考证

  ```
  SELECT 
  dept,
  userid,
  sal,
  PERCENT_RANK() OVER(ORDER BY sal) AS rn1,   --分组内
  RANK() OVER(ORDER BY sal) AS rn11,          --分组内RANK值
  SUM(1) OVER(PARTITION BY NULL) AS rn12,     --分组内总行数
  PERCENT_RANK() OVER(PARTITION BY dept ORDER BY sal) AS rn2 
  FROM itcast_t3;
  
  rn1: rn1 = (rn11-1) / (rn12-1) 
  	   第一行,(1-1)/(5-1)=0/4=0
  	   第二行,(2-1)/(5-1)=1/4=0.25
  	   第四行,(4-1)/(5-1)=3/4=0.75
  rn2: 按照dept分组，
       dept=d1的总行数为3
       第一行，(1-1)/(3-1)=0
       第三行，(3-1)/(3-1)=1
  ```






























