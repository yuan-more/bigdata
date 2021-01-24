### Hive分析窗口函数(一) SUM,AVG,MIN,MAX

- 数据准备

  ```
  建表语句:
  create table itcast_t1(
  cookieid string,
  createtime string,   --day 
  pv int
  ) row format delimited 
  fields terminated by ',';
  
  加载数据：
  load data local inpath '/root/hivedata/itcast_t1.dat' into table itcast_t1;
  
  cookie1,2018-04-10,1
  cookie1,2018-04-11,5
  cookie1,2018-04-12,7
  cookie1,2018-04-13,3
  cookie1,2018-04-14,2
  cookie1,2018-04-15,4
  cookie1,2018-04-16,4
  
  开启智能本地模式
  SET hive.exec.mode.local.auto=true;
  ```

---

- SUM（结果和ORDER BY相关,默认为升序）

  ```
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid order by createtime) as pv1 
  from itcast_t1;
  
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid order by createtime rows between unbounded preceding and current row) as pv2
  from itcast_t1;
  
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid) as pv3
  from itcast_t1;
  
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid order by createtime rows between 3 preceding and current row) as pv4
  from itcast_t1;
  
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid order by createtime rows between 3 preceding and 1 following) as pv5
  from itcast_t1;
  
  select cookieid,createtime,pv,
  sum(pv) over(partition by cookieid order by createtime rows between current row and unbounded following) as pv6
  from itcast_t1;
  
  
  pv1: 分组内从起点到当前行的pv累积，如，11号的pv1=10号的pv+11号的pv, 12号=10号+11号+12号
  pv2: 同pv1
  pv3: 分组内(cookie1)所有的pv累加
  pv4: 分组内当前行+往前3行，如，11号=10号+11号， 12号=10号+11号+12号，
  	                       13号=10号+11号+12号+13号， 14号=11号+12号+13号+14号
  pv5: 分组内当前行+往前3行+往后1行，如，14号=11号+12号+13号+14号+15号=5+7+3+2+4=21
  pv6: 分组内当前行+往后所有行，如，13号=13号+14号+15号+16号=3+2+4+4=13，
  							 14号=14号+15号+16号=2+4+4=10
  ```

  - 如果不指定rows between,默认为从起点到当前行;
  - 如果不指定order by，则将分组内所有值累加;
  - 关键是理解rows between含义,也叫做window子句：
    - preceding：往前
    - following：往后
    - current row：当前行
    - unbounded：起点
    - unbounded preceding 表示从前面的起点
    - unbounded following：表示到后面的终点

----

- AVG，MIN，MAX，和SUM用法一样

  ```
  select cookieid,createtime,pv,
  avg(pv) over(partition by cookieid order by createtime rows between unbounded preceding and current row) as pv2
  from itcast_t1;
  
  select cookieid,createtime,pv,
  max(pv) over(partition by cookieid order by createtime rows between unbounded preceding and current row) as pv2
  from itcast_t1;
  
  select cookieid,createtime,pv,
  min(pv) over(partition by cookieid order by createtime rows between unbounded preceding and current row) as pv2
  from itcast_t1;
  ```
























