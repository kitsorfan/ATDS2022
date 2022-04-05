# Advanced Topics in Database Systems

### Exercises in Python/SQL, semester project for Advanced Topics in Database Systems course at [ECE](https://www.ece.ntua.gr/en)⚡, [NTUA](https://www.ntua.gr/en)🎓, academic year 2021-2022

<img alt="Python" src = "https://img.shields.io/badge/Python-1136AA?style=for-the-badge&logo=python&logoColor=white" height="28"> <img alt="Spark SQL" src = "https://img.shields.io/badge/Spark SQL-important?style=for-the-badge&logo=apachespark&logoColor=white" height="28"> <img alt="Hadoop" src = "https://img.shields.io/badge/Hadoop-blue?style=for-the-badge&logo=apachehadoop&logoColor=black" height="28"> <img alt="Ubuntu Server" src = "https://img.shields.io/badge/Ubuntu Server-E95420?style=for-the-badge&logo=ubuntu&logoColor=white" height="28">

## 📋**Description**

The dataset used for this project is [Full MovieLens Dataset](http://www.cslab.ntua.gr/courses/atds/movie_data.tar.gz) .


The project consists of two main parts:
1) Implement and test 5 requested queries using RDD API and Spark SQL 
2) Do performance analysis for *Reduce-Side join*, *Map-Side join* implementations


**Details:**
- We used 3 VMs for our cluster ( 1 NameNode , 2 DataNodes )
- Dataset formats used: csv, dataframe, parquet


### Project Goals
- get familiar with Spark API
- evaluate performance for a list of queries
- compare different *join* algorithms in Spark Map-Reduce

Project's assignment and report are written in greek.

### 👔Team Members

| Name - GitHub                                     | Email                   |
|----------------------------------------------------------------|-------------------------|
| [Stylianos Kandylakis](https://github.com/stylkand/) |  <a href = "mailto:stelkcand@gmail.com" target="_blank"><img alt="gmail" src = "https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white">   |
| [Kitsos Orfanopoulos](https://github.com/kitsorfan)               | <a href = "mailto:kitsorfan@protonmail.com" target="_blank"><img alt="protonmail" src = "https://img.shields.io/badge/ProtonMail-8B89CC?style=for-the-badge&logo=protonmail&logoColor=white" ></a>|
| [Christos Tsoufis](https://github.com/ChristosTsoufis)                 | <a href = "mailto:chris99ts@gmail.com" target="_blank"><img alt="gmail" src = "https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white">      |



## 🖥**Specifications of VM**

|OS | CPUs |RAM |Disk space|  
|----|-----|-------| ------|   
|Ubuntu 16.04 LTS (Xenial)| 2 | 2GB|30GB|
  
### **🔗Sources**
- https://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html
- https://stg-tud.github.io/ctbd/2017/CTBD_06_gfs-hdfs.pdf
