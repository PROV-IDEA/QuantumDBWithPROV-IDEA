# QuantumDB with PROV-IDEA Approach

QuantumDB is a method for evolving database schemas of SQL databases, while not impeding access to SQL clients or
requiring downtime. PROV-IDEA is presented as a PROV-Interoperable Database Evolution Approach aimed at integrating provenance
information into relational database schema evolution, using PROV as provenance framework.

In this repository we present our approach for making the QuantumDB tool provenance-aware by applying PROV-IDEA

For more information, please checkout the link:

* [PROV-IDEA. Supplementary material. (2022)](https://zenodo.org/records/10701239)

## Setup environment

For testing QuantumDB with PROV-IDEA you'll want to:


      * Have a PostgreSQL database server running to test and run against. PostgreSQL is used by QuantumDB as database to be evolved. It should be running 
      on the default  URL postgres://localhost:5432. A database <database> has to be created.
      
      * Have a MongoDB database server running to test and run against. MongoDB is used by PROV-IDEA as database to store provenance data. It should be running 
      on the default  URL mongodb://localhost:27017, and with a 'PROVIDEA' database and an 'Evolution_Changes' collection.

## Building

To run the Maven build, do:


 ***mvn clean install -Dmaven.test.skip=true***
   

## Running

To test QuantumDB with PROV-IDEA we suggest using the QuantumDB CLI tool (recommended and easiest option to manage databases with QuantumDB). For this task, we suggest executing class Cli in package io.quantumdb.cli of quantumdb-cli module. The ***changelog.xml*** file is under quantumdb-cli module, which includes:
- a copy table change named **copy-1**, aimed at creating a new table named **developer_sub_b_old**, as a copy of the columns and data of a table called **developer_sub_b** (so, table **developer_sub_b** must exist in database <database>).
- a merge table change named **merge-2-developer_sub_a-developer_sub_b**, aimed at merging two tables (**developer_sub_a**  and **developer_sub_b**) into a new one named **developer_sub_a_new** (so, tables **developer_sub_a** and  **developer_sub_b** must exist in database <database>).
- a decompose table change named **decom-3-developer_sub_a_new**, aimed at decomposing a table named **developer_sub_a_new** into two ones named **developer_sub_a** and **contact** (so, table **developer_sub_a_new** must exist in database <database>).

To sequentially execute any of the changes use the followings commands:
     

- ***init --host=localhost:5432 --database=\<database\> --username=\<username\> --password=\<password\>***
    
- ***changelog***

- ***fork <change_name>***

