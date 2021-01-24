## zookeeper的介绍以及集群环境搭建

### 1.1 zookeeper概述

​	Zookeeper 是一个分布式协调服务的开源框架。 主要用来解决分布式集群中应用系统的一致性问题，例如怎样避免同时操作同一数据造成脏读的问题。

​	ZooKeeper 本质上是一个分布式的小文件存储系统。 提供基于类似于文件系统的目录树方式的数据存储，并且可以对树中的节点进行有效管理。从而用来维护和监控你存储的数据的状态变化。通过监控这些数据状态的变化，从而可以达到基于数据的集群管理。 诸如： 统一命名服务(dubbo)、分布式配置管理(solr的配置集中管理)、分布式消息队列（sub/pub）、分布式锁、分布式协调等功能。

### 1.2 zookeeper的架构图

![1560145739593](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560145739593.png)

- Leader:

```
Zookeeper 集群工作的核心
事务请求（写操作） 的唯一调度和处理者，保证集群事务处理的顺序性；
集群内部各个服务器的调度者。
对于 create， setData， delete 等有写操作的请求，则需要统一转发给leader 处理， leader 需要决定编号、执行操作，这个过程称为一个事务。

```

- Follower:

```
处理客户端非事务（读操作） 请求，

转发事务请求给 Leader；

参与集群 Leader 选举投票 2n-1台可以做集群投票。

此外，针对访问量比较大的 zookeeper 集群， 还可新增观察者角色。

```

- Observer:

```
观察者角色，观察 Zookeeper 集群的最新状态变化并将这些状态同步过
来，其对于非事务请求可以进行独立处理，对于事务请求，则会转发给 Leader
服务器进行处理。
不会参与任何形式的投票只提供非事务服务，通常用于在不影响集群事务
处理能力的前提下提升集群的非事务处理能力。

扯淡：说白了就是增加并发的读请求
```

### 1.3 zookeeper的特性

- 1) 全局数据一致：每个 server 保存一份相同的数据副本， client 无论连接到哪个 server，展示的数据都是一致的，这是最重要的特征；
- 2) 可靠性：如果消息被其中一台服务器接受，那么将被所有的服务器接受。
- 3) 顺序性：包括全局有序和偏序两种：全局有序是指如果在一台服务器上消息 a 在消息 b 前发布，则在所有 Server 上消息 a 都将在消息 b 前被发布；偏序是指如果一个消息 b 在消息 a 后被同一个发送者发布， a 必将排在 b 前面。
- 4) 数据更新原子性：一次数据更新要么成功（半数以上节点成功），要么失败，不存在中间状态；
- 5) 实时性： Zookeeper 保证客户端将在一个时间间隔范围内获得服务器的更新信息，或者服务器失效的信息。

### 1.4 三台机器zookeeper的集群环境搭建

​	Zookeeper 集群搭建指的是 ZooKeeper 分布式模式安装。 通常由 2n+1台 servers 组成。 这是因为为了保证 Leader 选举（基于 Paxos 算法的实现） 能过得到多数的支持，所以 ZooKeeper 集群的数量一般为奇数。Zookeeper 运行需要 java 环境， 所以需要提前安装 jdk。 对于安装leader+follower 模式的集群， 大致过程如下：

- 配置主机名称到 IP 地址映射配置
- 修改 ZooKeeper 配置文件
- 远程复制分发安装文件
- 设置 myid
- 启动 ZooKeeper 集群

如果要想使用 Observer 模式，可在对应节点的配置文件添加如下配置：peerType=observer
其次，必须在配置文件指定哪些节点被指定为 Observer，如：server.1:localhost:2181:3181:observer

| 服务器IP       | 主机名 | myid的值 |
| -------------- | ------ | -------- |
| 192.168.52.100 | node01 | 1        |
| 192.168.52.110 | node02 | 2        |
| 192.168.52.120 | node03 | 3        |

- 第一步:  下载zookeeper压缩包, 下载网站如下:

```
http://archive.apache.org/dist/zookeeper/
我们在这个网址下载我们使用的zk版本为3.4.9
下载完成之后，上传到我们的linux的/export/softwares路径下准备进行安装
```

- 第二步: 解压

【解压zookeeper的压缩包到/export/servers路径下去，然后准备进行安装】

```
cd /export/softwares
tar -zxvf zookeeper-3.4.9.tar.gz -C ../servers/
```

![1560146433858](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560146433858.png)

- 第三步: 修改配置文件

【第一台机器修改配置文件】

```
cd /export/servers/zookeeper-3.4.9/conf/
cp zoo_sample.cfg zoo.cfg
mkdir -p /export/servers/zookeeper-3.4.5-cdh5.14.0/zkdatas/
vim  zoo.cfg
```

![1560146571438](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560146571438.png)

- 第四步: 添加myid配置

【第一台机器中】

```
/export/servers/zookeeper-3.4.9/zkdatas/ 这个路径下创建一个文件，文件名为myid ,文件内容为1

echo 1 > /export/servers/zookeeper-3.4.9/zkdatas/myid 
```

![1560146781825](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560146781825.png)

- 第五步: 安装包分发并修改myid的值

```
安装包分发到其他机器

第一台机器上面执行以下两个命令

scp -r  /export/servers/zookeeper-3.4.9/ node02:/export/servers/

scp -r  /export/servers/zookeeper-3.4.9/ node03:/export/servers/

第二台机器上修改myid的值为2

echo 2 > /export/servers/zookeeper-3.4.9/zkdatas/myid

```

![1560146833673](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560146833673.png)

```
第三台机器上修改myid的值为3
echo 3 > /export/servers/zookeeper-3.4.9/zkdatas/myid
```

![1560146862650](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560146862650.png)

- 第六步:  三台机器启动zookeeper服务

```
三台机器启动zookeeper服务: 这个命令三台机器都要执行
/export/servers/zookeeper-3.4.9/bin/zkServer.sh start

查看启动状态
/export/servers/zookeeper-3.4.9/bin/zkServer.sh  status
```

### 1.5 zookeeper的数据模型

​	ZooKeeper 的数据模型，在结构上和标准文件系统的非常相似，拥有一个层次的命名空间，都是采用树形层次结构，ZooKeeper 树中的每个节点被称为—Znode。和文件系统的目录树一样，ZooKeeper 树中的每个节点可以拥有子节点。但也有不同之处：

​	1. Znode 兼具文件和目录两种特点。既像文件一样维护着数据、元信息、ACL、 时间戳等数据结构，又像目录一样可以作为路径标识的一部分，并可以具有 子 Znode。用户对 Znode 具有增、删、改、查等操作（权限允许的情况下）。

​	2. Znode 具有原子性操作，读操作将获取与节点相关的所有数据，写操作也将 替换掉节点的所有数据。另外，每一个节点都拥有自己的 ACL(访问控制列表)，这个列表规定了用户的权限，即限定了特定用户对目标节点可以执行的操作。

​	3. Znode 存储数据大小有限制。ZooKeeper 虽然可以关联一些数据，但并没有 被设计为常规的数据库或者大数据存储，相反的是，它用来管理调度数据， 比如分布式应用中的配置文件信息、状态信息、汇集位置等等。这些数据的 共同特性就是它们都是很小的数据，通常以 KB 为大小单位。ZooKeeper 的服 务器和客户端都被设计为严格检查并限制每个 Znode 的数据大小至多 1M，常规使用中应该远小于此值。

```
可以通过在zkServer.sh 中  添加一个参数 :  ZOO_USER_CFG="-Djute.maxbuffer=10240000" 
修改znode数据大小,单位为字节
```

​	4. Znode 通过路径引用，如同 Unix 中的文件路径。路径必须是绝对的，因此他们必须由斜杠字符来开头。除此以外，他们必须是唯一的，也就是说每一个 路径只有一个表示，因此这些路径不能改变。在 ZooKeeper 中，路径由 Unicode 字符串组成，并且有一些限制。字符串"/zookeeper"用以保存管理 信息，比如关键配额信息。

#### 1.5.1 数据结构

![1560150643239](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560150643239.png)

图中的每个节点称为一个 Znode。 每个 Znode 由 3 部分组成:	

​	① stat：此为状态信息, 描述该 Znode 的版本, 权限等信息

​	② data：与该 Znode 关联的数据

​	③ children：该 Znode 下的子节点

#### 1.5.2 节点类型

Znode 有两种，分别为临时节点和永久节点。节点的类型在创建时即被确定，并且不能改变。

临时节点：该节点的生命周期依赖于创建它们的会话。一旦会话结束，临时节点将被自动删除，当然可以也可以手动删除。临时节点不允许拥有子节点。

永久节点：该节点的生命周期不依赖于会话，并且只有在客户端显示执行删除操作的时候，他们才能被删除。

Znode 还有一个序列化（顺序）的特性，如果创建的时候指定的话，该 Znode 的名字后面会自动追加一个不断增加的序列号。序列号对于此节点的父节点来说是唯一的，这样便会记录每个子节点创建的先后顺序。它的格式为“%10d”(10 位数字，没有数值的数位用 0 补充，例如“0000000001”)。

![1560150914340](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560150914340.png)

这样便会存在四种类型的 Znode 节点，分别对应：

PERSISTENT：永久节点  

EPHEMERAL：临时节点

PERSISTENT_SEQUENTIAL：永久节点、序列化  sequential

EPHEMERAL_SEQUENTIAL：临时节点、序列化

```
创建永久节点：
[zk: localhost:2181(CONNECTED) 3] create /hello world
Created /hello 
创建临时节点：
[zk: localhost:2181(CONNECTED) 5] create -e /abc 123
Created /abc
创建永久序列化节点：
[zk: localhost:2181(CONNECTED) 6] create -s /zhangsan boy
Created /zhangsan0000000004
创建临时序列化节点：
zk: localhost:2181(CONNECTED) 11] create -e -s /lisi boy
Created /lisi0000000006
```

![1560151210312](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560151210312.png)

#### 1.5.3 节点属性

​	每个 znode 都包含了一系列的属性，通过命令 get，可以获得节点的属性。

![1560151552822](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560151552822.png)

​	dataVersion：数据版本号，每次对节点进行 set 操作，dataVersion 的值都会增加 1（即使设置的是相同的数据），可有效避免了数据更新时出现的先后顺序问题。

​	cversion ：子节点的版本号。当 znode 的子节点有变化时，cversion 的值就会增加 1。

​	aclVersion ：ACL 的版本号。

​	cZxid ：Znode 创建的事务 id。

​	mZxid	：Znode 被修改的事务 id，即每次对 znode 的修改都会更新 mZxid。

 	对于 zk 来说，每次的变化都会产生一个唯一的事务 id，zxid（ZooKeeper Transaction Id）。通过 zxid，可以确定更新操作的先后顺序。例如，如果 zxid1小于 zxid2，说明 zxid1 操作先于 zxid2 发生，zxid 对于整个 zk 都是唯一的，即使操作的是不同的 znode。

​	ctime：节点创建时的时间戳.

​	mtime：节点最新一次更新发生时的时间戳.

​	ephemeralOwner:如果该节点为临时节点, ephemeralOwner 值表示与该节点绑定的 session id. 如果不是, ephemeralOwner 值为 0.

​	在 client 和 server 通信之前,首先需要建立连接,该连接称为 session。连接建立后,如果发生连接超时、授权失败,或者显式关闭连接,连接便处于 CLOSED状态, 此时 session 结束。

### 1.6 zookeeper的shell操作

#### 1.6.1 客户端连接

运行 zkCli.sh –server ip 进入命令行工具。输入 help，输出 zk shell 提示：

![1560147016475](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560147016475.png)

#### 1.6.2 shell操作

- 创建节点:
  - 格式:  create \[-s][-e] path data acl
    - 其中，-s 或-e 分别指定节点特性，顺序或临时节点，若不指定，则表示持 久节点；acl 用来进行权限控制。

【创建顺序节点】 

![1560147199027](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560147199027.png)

【创建临时节点】

![1560147243699](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560147243699.png)

【创建持久节点】

![1560147273607](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560147273607.png)

- 2) 读取节点
  - 与读取相关的命令有 ls  命令和 get  命令，ls 命令可以列出 Zookeeper 指定节点下的所有子节点，只能查看指定节点下的第一级的所有子节点；get 命令可以获取 Zookeeper 指定节点的数据内容和属性信息。
  - 格式:  
    - ls path [watch]      
    - get path [watch]
    - ls2 path [watch]

![1560147662321](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560147662321.png)

- 3) 更新索引 ：
  - 格式: set path data [version]
    - data 就是要更新的新内容，version 表示数据版本。

![1560148089214](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560148089214.png)

​	现在 dataVersion 已经变为 1 了，表示进行了更新。

- 4) 删除节点

  - 格式: delete path [version]

    - 若删除节点存在子节点，那么无法删除该节点，必须先删除子节点，再删除

      父节点。

    - rmr path:  可以递归删除节点。

- 5) 对节点进行限制: quota

  - 格式1:  setquota -n|-b val path 
    - n:表示子节点的最大个数 
    - b:表示数据值的最大长度 
    - val:子节点最大个数或数据值的最大长度 
    - path:节点路径

  ![1560148481689](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560148481689.png)

  - 格式2: listquota path : 列出指定节点的 quota

  ![1560148708091](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560148708091.png)

  ​				子节点个数为 2,数据长度-1 表示没限制 

  ```
  在实际操作的时候, 虽然设置了最大的节点数后,依然可以在整个节点下添加多个子节点, 只是会在zookeeper中的日志文件中记录一下警告信息
  
  ```

  ![1561189613947](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1561189613947.png)

  - 格式3: delquota [-n|-b] path    :  删除 quota



- 6) 其他命令:

  - history: 列出命令历史

  ![1560149685838](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560149685838.png)

  - redo：该命令可以重新执行指定命令编号的历史命令,命令编号可以通过

### 1.7 zookeeper的watch机制

​	ZooKeeper 提供了分布式数据发布/订阅（pub/sub）功能，一个典型的发布/订阅模型系统定义了一种一对多的订阅关系，能让多个订阅者同时监听某一个主题对象，当这个主题对象自身状态变化时，会通知所有订阅者，使他们能够做出相应的处理。ZooKeeper 中，引入了 Watcher 机制来实现这种分布式的通知功能 。ZooKeeper 允许客户端向服务端注册一个 Watcher 监听，当服务端的一些事件触发了这个 Watcher，那么就会向指定客户端发送一个事件通知来实现分布式的通知功能。触发事件种类很多，如：节点创建，节点删除，节点改变，子节点改变等。

​	总的来说可以概括 Watcher 为以下三个过程：客户端向服务端注册 Watcher、服务端事件发生触发 Watcher、客户端回调 Watcher 得到触发事件情况

#### 1.7.1 watch机制特点

- 一次性触发
  - 事件发生触发监听，一个 watcher event 就会被发送到设置监听的客户端，这种效果是一次性的，后续再次发生同样的事件，不会再次触发。
- 事件封装
  - ZooKeeper 使用 WatchedEvent 对象来封装服务端事件并传递。
  - WatchedEvent 包含了每一个事件的三个基本属性：
    - 通知状态（keeperState）
    - 事件类型（EventType）
    - 节点路径（path）
- event 异步发送
  - watcher 的通知事件从服务端发送到客户端是异步的。
- 先注册再触发
  - Zookeeper 中的 watch 机制，必须客户端先去服务端注册监听，这样事件发送才会触发监听，通知给客户端。

#### 1.7.2 通知状态和事件类型

​	同一个事件类型在不同的通知状态中代表的含义有所不同，下表列举了常见的通知状态和事件类型。

![1561191260909](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1561191260909.png)

其中**连接状态事件(type=None, path=null)不需要客户端注册**，客户端只要有需要直接处理就行了

#### 1.7.3 shell 客户端设置watch机制

设置节点数据变动监听：

```
get /aaa000000001 watch
```

![1560152369487](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560152369487.png)

通过另一个客户端更改节点数据：

```
set /aaa0000000001 hello22
```

![1560152391491](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560152391491.png)

此时设置监听的节点收到通知：

![1560152411452](D:/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E8%AF%BE%E7%A8%8B2/01-05%E9%A2%84%E4%B9%A0%E8%B5%84%E6%96%99/%E7%A7%8D%E5%AD%90%E6%96%87%E4%BB%B6/%E8%AF%BE%E4%BB%B6/assets/1560152411452.png)

### 1.8 zookeeper的javaAPI

​	Zookeeper 是在 Java 中客户端主类，负责建立与 zookeeper 集群的会话，并提供方法进行操作。

org.apache.zookeeper.Watcher

​	Watcher 接口表示一个标准的事件处理器，其定义了事件通知相关的逻辑，包含 KeeperState 和 EventType 两个枚举类，分别代表了通知状态和事件类型，同时定义了事件的回调方法：process（WatchedEvent event）。

​	process 方法是 Watcher 接口中的一个回调方法，当 ZooKeeper 向客户端发送一个 Watcher 事件通知时，客户端就会对相应的 process 方法进行回调，从而实现对事件的处理。

#### 1.8.1 创建java工程,导入jar包

​	创建maven  java工程，导入jar包 kju

```xml
<dependencies>
    	<dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-framework</artifactId>
            <version>2.12.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-recipes</artifactId>
            <version>2.12.0</version>
        </dependency>  
		<dependency>
		    <groupId>com.google.collections</groupId>
		    <artifactId>google-collections</artifactId>
		    <version>1.0</version>
		</dependency>
  </dependencies>
  <build>
		<plugins>
			<!-- java编译插件 -->
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.2</version>
				<configuration>
					<source>1.8</source>
					<target>1.8</target>
					<encoding>UTF-8</encoding>
				</configuration>
			</plugin>
		</plugins>
	</build>
```

#### 1.8.2 节点操作

创建永久节点

```java
/**
	 * 创建永久节点
	 * @throws Exception
	 */
	@Test
	public void createNode() throws Exception {
		RetryPolicy retryPolicy = new  ExponentialBackoffRetry(1000, 1);
//获取客户端对象
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.52.100:2181,192.168.52.110:2181,192.168.52.120:2181", 1000, 1000, retryPolicy);
//调用start开启客户端操作
		client.start();
	//通过create来进行创建节点，并且需要指定节点类型 
	client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello3/world");
client.close();
	}
```

创建临时节点

```java
	/**
	 * 创建临时节点
	 * @throws Exception
	 */
	@Test
	public void createNode2() throws Exception {
		RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
		CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
		client.start();
client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello5/world");
		Thread.sleep(5000);
		client.close();
	}
```

修改节点数据

```java
	/**
	 * 节点下面添加数据与修改是类似的，一个节点下面会有一个数据，新的数据会覆盖旧的数据
	 * @throws Exception
	 */
	@Test
	public void nodeData() throws Exception {
		RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
		CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
		client.start();
		client.setData().forPath("/hello5", "hello7".getBytes());
		client.close();
	}
```

节点数据查询

```java
	/**
	 * 数据查询
	 */
	@Test
	public void updateNode() throws Exception {
		RetryPolicy retryPolicy = new  ExponentialBackoffRetry(3000, 1);
		CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
		client.start();
		byte[] forPath = client.getData().forPath("/hello5");
		System.out.println(new String(forPath));
		client.close();
	}
```

节点watch机制

```java
/**
	 * zookeeper的watch机制
	 * @throws Exception 
	 */
	@Test
	public void watchNode() throws Exception {
		RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", policy);
		client.start();
		// ExecutorService pool = Executors.newCachedThreadPool();  
	        //设置节点的cache  
	        TreeCache treeCache = new TreeCache(client, "/hello5");  
	        //设置监听器和处理过程  
	        treeCache.getListenable().addListener(new TreeCacheListener() {  
	            @Override  
	            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {  
	                ChildData data = event.getData();  
	                if(data !=null){  
	                    switch (event.getType()) {  
	                    case NODE_ADDED:  
	                        System.out.println("NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));  
	                        break;  
	                    case NODE_REMOVED:  
	                        System.out.println("NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));  
	                        break;  
	                    case NODE_UPDATED:  
	                        System.out.println("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));  
	                        break;  
	                          
	                    default:  
	                        break;  
	                    }  
	                }else{  
	                    System.out.println( "data is null : "+ event.getType());  
	                }  
	            }  
	        });  
	        //开始监听  
	        treeCache.start();  
	        Thread.sleep(50000000);
	}
```

