# Insight Analytic(old carddeck)

## Input data

### Data Structure

#### Interaction data

Input data is structured as UCI, short for User, Content, Interaction. Unique id for user and content must be supplied for aggregation. Category is a list field

For detailed defination, please look at: [input_data.py](https://git.vionlabs.com/)

Look at an example of the input data.

| category         | actionType | channelID | duration | firstEvent       | inventoryID | runtime | title   | season | episode | userID |
|------------------|------------|-----------|----------|------------------|-------------|---------|---------|--------|---------|--------|
| drama            | vod        | null      | 159      | 2017-03-03 19:33 | 342857      | 7200    | Gravity | null   | null    | 233212 |
| [action, comedy] | live       | TV4       | 1999     | 2017-03-03 19:33 | 107519      | 5600    | Veep    | 1      | 11      | 233212 |

#### User data

## Module: Diagnostic

## Sub-modules

### data_interface

Reading interactions objects from spark parquet, possibly also write to rethink-api database

### calculation

All aggregation and transformation of ucis are done here

###  execute

Calling to execute all the aggregations
