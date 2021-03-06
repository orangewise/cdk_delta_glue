cdk_delta_glue
==============

Run [Delta Lake](https://delta.io) in AWS Glue using the [Cloud Development Kit](https://aws.amazon.com/cdk/).

# Overview

![overview](img/dms-glue-delta-lake-overview.png)

## Usage

```bash

# install cdk and python deps
./install.sh

# deploy stack to your account (first change the profile in the ./deploy.sh)
./deploy.sh
```

# DMS Dummy Data

## Full load

```bash
parquet-tools show assets/dms/full/data_ronald/LOAD00000001.parquet

+----------------------------+------+---------------------+-----------+---------+
| TIMESTAMP                  |   id | datetime            |   channel |   value |
|----------------------------+------+---------------------+-----------+---------|
| 2021-06-11 08:28:55.010260 |    2 | 2021-05-07 09:21:25 |        24 |      12 |
| 2021-06-11 08:28:55.017027 |    3 | 2021-05-07 09:21:25 |        26 |      13 |
| 2021-06-11 08:28:55.017043 |    4 | 2021-05-07 09:21:25 |        28 |      14 |
+----------------------------+------+---------------------+-----------+---------+
```

## Changes

```bash
parquet-tools show assets/dms/cdc/data_ronald/2021/06/11/20210611-084409279.parquet

+------+----------------------------+------+---------------------+-----------+---------+
| Op   | TIMESTAMP                  |   id | datetime            |   channel |   value |
|------+----------------------------+------+---------------------+-----------+---------|
| U    | 2021-06-11 08:43:04.000000 |    2 | 2021-05-07 09:21:25 |        48 |      12 |
+------+----------------------------+------+---------------------+-----------+---------+

parquet-tools show assets/dms/cdc/data_ronald/2021/06/11/20210611-084751679.parquet

+------+----------------------------+------+---------------------+-----------+---------+
| Op   | TIMESTAMP                  |   id | datetime            |   channel |   value |
|------+----------------------------+------+---------------------+-----------+---------|
| I    | 2021-06-11 08:46:47.000000 |    1 | 2021-06-11 08:46:47 |        11 |      11 |
| D    | 2021-06-11 08:47:42.000000 |    1 | 2021-06-11 08:46:47 |        11 |      11 |
+------+----------------------------+------+---------------------+-----------+---------+

parquet-tools show assets/dms/cdc/data_ronald/2021/06/11/20210611-084932768.parquet

+------+----------------------------+------+---------------------+-----------+---------+
| Op   | TIMESTAMP                  |   id | datetime            |   channel |   value |
|------+----------------------------+------+---------------------+-----------+---------|
| I    | 2021-06-11 08:48:30.000000 |    1 | 2021-06-11 08:48:30 |       100 |     200 |
| I    | 2021-06-11 08:48:30.000000 |   10 | 2021-06-11 08:48:30 |      1000 |    2000 |
+------+----------------------------+------+---------------------+-----------+---------+
```

## Table after the merge

```bash
+----------------------------+------+---------------------+-----------+---------+
| TIMESTAMP                  |   id | datetime            |   channel |   value |
+----------------------------+------+---------------------+-----------+---------+
| 2021-06-11 08:48:30.000000 |    1 | 2021-06-11 10:48:30 |       100 |     200 |
| 2021-06-11 08:43:04.000000 |    2 | 2021-05-07 11:21:25 |        48 |      12 |
| 2021-06-11 08:28:55.017027 |    3 | 2021-05-07 11:21:25 |        26 |      13 |
| 2021-06-11 08:28:55.017043 |    4 | 2021-05-07 11:21:25 |        28 |      14 |
| 2021-06-11 08:48:30.000000 |   10 | 2021-06-11 10:48:30 |      1000 |    2000 |
+----------------------------+------+---------------------+-----------+---------+
```


# Athena Table

Create the Athena table by running the following sql statement (in the sAWS Athena console):

```sql
CREATE EXTERNAL TABLE `data_ronald` (
  `id` bigint COMMENT '',
  `datetime` timestamp COMMENT '',
  `channel` bigint COMMENT '',
  `value` float COMMENT ''
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://<YOUR_ACCOUNT_ID>-cdk-delta-glue/delta/data_ronald/_symlink_format_manifest/'
```

# Query the table

![overview](img/delta-athena-table.png)
