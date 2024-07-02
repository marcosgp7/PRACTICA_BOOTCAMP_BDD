# PRACTICA_BOOTCAMP_BDD
This is a repository for database work using dbt and snowflake.

The work is divided into 6 parts. Each part needs the resolution of the previous one to continue.

## _PART A_

First do a DATASET study to understand the information provided by each table. 
There is NO need to document this study unless it is deemed necessary to justify the logic being applied. 
logic being applied.

You will work with the Snowflake test dataset TCPH_SF1.

## _PART B_

RAW data layer

Creates a "RAW" data layer that is a subset obtained from the DATASET simulating a "fake" data generation. 
A company like this, which delivers shipments, will periodically receive orders in its database. 
database. You must build RAW tables little by little by programming inserts to simulate this flow. 
simulate this flow. In addition, you can incorporate data from other sources or generate them yourself according to the 
as needs arise.

## _PART C_

Intermediate data layer.

This layer should contain the dimensions and fact tables with clean data and applying some logic. 
some logic.

Create a table that is sales at date level. This table should report, among other things, 
the units (along with the amount) of each item purchased (or returned) by each customer on each day. 
customer on each day. It is necessary to perform some data cleansing, depending on the needs that arise, e.g., for example 
for example, in the case of using the O_CLERK field, a possible cleanup would be to keep the numeric part of the data 
would be to keep the numeric part, obtaining data of type NUMBER.
