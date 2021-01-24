/*
SQLyog Ultimate v8.32 
MySQL - 5.6.22-log : Database - sql_prac
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`sql_prac` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;

USE `sql_prac`;

/*Table structure for table `course` */

DROP TABLE IF EXISTS `course`;

CREATE TABLE `course` (
  `c_id` varchar(32) DEFAULT NULL,
  `c_name` varchar(32) DEFAULT NULL,
  `t_id` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*Data for the table `course` */

insert  into `course`(`c_id`,`c_name`,`t_id`) values ('01','语文','02'),('02','数学','01'),('03','英语','03');

/*Table structure for table `score` */

DROP TABLE IF EXISTS `score`;

CREATE TABLE `score` (
  `s_id` varchar(32) DEFAULT NULL,
  `c_id` varchar(32) DEFAULT NULL,
  `s_score` int(8) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*Data for the table `score` */

insert  into `score`(`s_id`,`c_id`,`s_score`) values ('01','01',80),('01','02',90),('01','03',99),('02','01',70),('02','02',60),('02','03',80),('03','01',80),('03','02',80),('03','03',80),('04','01',50),('04','02',30),('04','03',20),('05','01',76),('05','02',87),('06','01',31),('03','03',34),('07','02',89),('07','03',98);

/*Table structure for table `student` */

DROP TABLE IF EXISTS `student`;

CREATE TABLE `student` (
  `s_id` varchar(32) NOT NULL,
  `s_name` varchar(32) DEFAULT NULL,
  `s_birth` varchar(32) DEFAULT NULL,
  `s_sex` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`s_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*Data for the table `student` */

insert  into `student`(`s_id`,`s_name`,`s_birth`,`s_sex`) values ('01','赵雷','1990-01-01','男'),('02','钱电','1990-12-21','男'),('03','孙风','1990-05-20','男'),('04','李云','1990-08-06','男'),('05','周梅','1991-12-01','女'),('06','吴兰','1992-03-01','女'),('07','郑竹','1989-07-01','女'),('08','王菊','1990-01-20','女');

/*Table structure for table `techer` */

DROP TABLE IF EXISTS `techer`;

CREATE TABLE `techer` (
  `t_id` varchar(32) DEFAULT NULL,
  `t_name` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*Data for the table `techer` */

insert  into `techer`(`t_id`,`t_name`) values ('01','张三'),('02','李四'),('03','王五');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
