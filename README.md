# Spark Insight Analytic

## Input data

### Data Structure

#### Data structure

Input data is structured as UCI, short for User, Content, Interaction. Unique id for user and content must be supplied for aggregation. Category is a list field

For detailed defination, please look at: [input_data.py](https://git.vionlabs.com/)

##### Interaction data

Look at an example of the interaciton data.

| category         | actionType | channelID | duration | firstEvent       | inventoryID | runtime | title   | season | episode | userID |
|------------------|------------|-----------|----------|------------------|-------------|---------|---------|--------|---------|--------|
| drama            | vod        | null      | 159      | 2017-03-03 19:33 | 342857      | 7200    | Gravity | null   | null    | 233212 |
| [action, comedy] | live       | TV4       | 1999     | 2017-03-03 19:33 | 107519      | 5600    | Veep    | 1      | 11      | 233212 |

##### User data

Look at an example of user data.

| firstActivity    | userID | basicPackage | additionalPackages |
|------------------|--------|--------------|--------------------|
| 2017-03-03 19:33 | 233212 | ["T1"]       | ["Netflix", "HBO"] |
| 2017-03-03 19:33 | 233212 | ["T2"]       | ["Hulu"]           |

##### Content data

Content data is objects flattened in rethinkdb.

## Output

Output is stored in databases(currently in rethinkdb, could be Mongo, etc), every queryable tag has its own table, and datetime, tag_name should all be indexed in the db for query.

## Module: Diagnostic

## Sub-modules

### data_interface

Reading interactions objects from spark parquet, possibly also write to rethink-api database

### calculation

All aggregation and transformation of ucis are done here

###  execute

Calling to execute all the aggregations
