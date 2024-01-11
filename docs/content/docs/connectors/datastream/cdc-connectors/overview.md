---
title: Overview
weight: 1
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CDC Connectors

## Supported Connectors

| Connector                                                                                 | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                    |
|-------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| [mongodb-cdc](../docs/cdc/mongodb-cdc.md)                                                 | <li> [MongoDB](https://www.mongodb.com): 3.6, 4.x, 5.0                                                                                                                                                                                                                                                                                                                                                 | MongoDB Driver: 4.3.4     |
| [mysql-cdc](../../../docs/content/docs/connectors/datastream/cdc-connectors/mysql-cdc.md) | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.28       |
| [oceanbase-cdc](../docs/cdc/oceanbase-cdc.md)                                             | <li> [OceanBase CE](https://open.oceanbase.com): 3.1.x, 4.x <li> [OceanBase EE](https://www.oceanbase.com/product/oceanbase): 2.x, 3.x, 4.x                                                                                                                                                                                                                                                            | OceanBase Driver: 2.4.x   |
| [oracle-cdc](../docs/cdc/oracle-cdc.md)                                                   | <li> [Oracle](https://www.oracle.com/index.html): 11, 12, 19, 21                                                                                                                                                                                                                                                                                                                                       | Oracle Driver: 19.3.0.0   |
| [postgres-cdc](../docs/cdc/postgres-cdc.md)                                               | <li> [PostgreSQL](https://www.postgresql.org): 9.6, 10, 11, 12, 13, 14                                                                                                                                                                                                                                                                                                                                 | JDBC Driver: 42.5.1       |
| [sqlserver-cdc](../docs/cdc/sqlserver-cdc.md)                                             | <li> [Sqlserver](https://www.microsoft.com/sql-server): 2012, 2014, 2016, 2017, 2019                                                                                                                                                                                                                                                                                                                   | JDBC Driver: 9.4.1.jre8   | 
| [tidb-cdc](../docs/cdc/tidb-cdc.md)                                                       | <li> [TiDB](https://www.pingcap.com/): 5.1.x, 5.2.x, 5.3.x, 5.4.x, 6.0.0                                                                                                                                                                                                                                                                                                                               | JDBC Driver: 8.0.27       | 
| [db2-cdc](../docs/cdc/db2-cdc.md)                                                         | <li> [Db2](https://www.ibm.com/products/db2): 11.5                                                                                                                                                                                                                                                                                                                                                     | Db2 Driver: 11.5.0.0      |
| [vitess-cdc](../docs/cdc/vitess-cdc.md)                                                   | <li> [Vitess](https://vitess.io/): 8.0.x, 9.0.x                                                                                                                                                                                                                                                                                                                                                        | MySql JDBC Driver: 8.0.26 |

## Supported Pipeline Connectors

{{< top >}}

