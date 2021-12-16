# foreach-db
Powershell module to execute SQL scripts in each database in a single or multiple servers.
This module allows the user to:

* Execute scripts that produce no result set, such as alter(s), create, etc.
* Execute scripts that produce an output. Multiple formats are supported
* Select data from each database processed and "pump" it to a single target table

# Documentation

## SYNOPSIS
  This script executes commands in a .sql script file or parameter in all databases
  present in the specified SQL instance or provided by a custom query in multiple server instances.
  It has the ability to produce a single output file in any of multiple supported formats 
  (CSV, TAB delimited, Pipe delimited, XLSX, JSON, XML) or pump the data to a single table in a 
  specific server in a specific database.

## DESCRIPTION
  If any error occurs the script reports the database that failed and the process continues.
  If -dbErrorsFileOutput or -dbWarningsFileOutput are provided, the database names of the
  databases that caused the errors or warnings are written the files specified in these params.
  If the parameter -dbExceptionFileInput is provided file specified in this parameter should
  contains a list of database names and regex matching server names
  to be skipped when processing. This is used to avoid processing databases known not to be 
  incompatible with the script.
  Authentication method used current user in Active Directory.
  Default level of parallelism if -parallelLevel is not provided is 4 meaning there will be 4
  concurrent jobs executing the script.
  In case Format is not specified while a result set file output is specified, the script will attempt
  to derive the format type from the file extension otherwise defaulting to CSV.

 ### Parameter Server
  Name of the MSSQL server instance to connect to.

 ### Parameter File
  Filename with a .SQL script to execute against the matched databases.

 ### Parameter DBExceptionFileInput
  File name containing databases to ignore when processing the command provided
  in -Query or -File parameter. The format is:
  dbname1=regex to match against server name
  dbname2=regex to match against server name
  Please notice in this file there must be only one entry per DBName. If you need to match a single database name
  against multiple databases you need to solve this with a regular expression. For example, the following entry will
  avoid processing database name TEST against ANY server:
  TEST=.*

 ### Parameter ParallelLevel
  Level of parallelism used to execute the parameters. Bu default the value of
  this parameter is 4, meaning 4 parallel processes are going to be spawned and
  the output of this jobs is going to be collected and reported in a single 
  pipeline. You need to experiment with this parameter to find the best performance depending on your workload.

 ### Parameter DBWarningsFileOutput
  Filename where to output database names of databases where the provided script
  errored out with special RAISERROR used to denote a warning. The type of exception
  raised is as follows:
	RAISERROR('Not a compatible database', 20, -1) with log

 ### Parameter DBErrorsFileOutput
  Filename where to output database names of databases where the provided script
  errored out when executing the provided statements.

 ### Parameter DetailedErrorLogFileOutput
  If provided all errors resulting from SQL commands execution will be logged to this file in JSON format. The objects will
  contain the database name where the error was captured and the resulting error message.

 ### Parameter Query
  Query/command to execute against matching databases

 ### Parameter ResultSetFormat
  Format to use when query returns data (SELECT statement). Valid formats are:
    CSV - Comma delimited output
    TAB - TAB character delimited output
    XLSX - Native Excel file format
    JSON - JSON array of objects
    PIPE - Pipe delimited file
    DELIMITED - A delimited file with delimiter specified by -Delimiter parameter
    HTML - Produces an HTML file
    XML - Xml format

 ### Parameter Delimiter
  When using -ResultSetFormat = DELIMITED -Delimiter specifies the character to use as delimiter in the output

 ### Parameter ResultSetFileOutput
  File name of target file where to output result sets returned from SELECT calls.
  Result set will also be shown on the console.

 ### Parameter OpenExcel
  If specified and if output format type is CSV or XLSX Excel will be opened with the target
  file specified in ResultSetFileOutput

 ### Parameter ReRunDBFromErrorsFile
  If this switch is specified the script will re-run all databases contained in the file provided in DbErrorsFileOutput parameter.
  Please notice the format of every entry in this file is <DBName>=<ServerName>. The match for this feature has to be exact in contrast
  with the setting -DBExceptionFileInput which takes a regular expression at the right side of the equal sign on the DBNAME=SERVERNAME 
  entries. 

 ### Parameter BindVariables
  Pass here an array of key value pairs of SQLCMD variables and their corresponding values. 
  The format for each element in the array is as follows: VARIABLE=VALUE.
  See examples for more details

 ### Parameter DBDriverQueryFile
  File name of file containing a driver query returning the server names, database names and buckets to split the list of target databases
  to execute the query or script. The query must return the following fields:
    * BUCKET
    * DBNAME
    * SERVERNAME
  This is the query used by default pulling databases from MSSQL metadata:
    SELECT (ROW_NUMBER() OVER (ORDER BY NAME) - 1) % {parallelLevel} BUCKET, NAME AS DBNAME, 'vm-pc-sql02' SERVERNAME FROM sys.databases
  When using a custom query make sure to use the variable $parallelLevel in order to create matching buckets for the list of databases.

 ### Parameter ConsoleDuplex
  When this switch is specified together with ResultSetFileOutput the result set generated will be also output to console. This will
  slow down processing when using parallel level > 1.

 ### Parameter ShowEachExecSummary
  This switch controls if showing summary after every database job is processed.

 ### Parameter ShowParams
  This switch enables showing the values of all parameters passed to this cmdlet

 ### Parameter ConsoleSilence
  Controls if displaying error messages, final summary per job and total time elapsed. If ConsoleDuplex us enabled result sets will be
  shown in the console ignoring ConsoleSilence. If not ResultSetFileOutput is specified result sets will also be output to console.

 ### Parameter PumpToServer
  Specifies the target server that contains the target database and target table to output the aggregate result set. 
  If ommited it will default to the Server parameter.

 ### Parameter PumpToDatabase
  Target database in the target server containing the table where to output the aggregate result set.

 ### Parameter PumpToTable
  Target table where to insert the records produced by the result set.

 ### Parameter PumpToTableInsertFields
  Array of strings containing the fields to be part of the insert statement used to output into the target table.

 ### Parameter PumpBulkSize
  Number of records to insert into the target table per operation. Default value is 50
  When setting this parameter take into account that the maximum supported rows for an insert statement is 1000.

# Examples

  Executes the default query SELECT DB_NAME() DBNAME in each database of server vm-pc-sql02 
  ```
ForEach-DB -Server vm-pc-sql02
```

  When using a SELECT statement it's possible to output directly to an Excel compatible .csv file.
```  
ForEach-DB -server vm-pc-sql02 -query "select db_name(), getdate()" -ResultSetFormat CSV -out output.csv
```
  The following example runs query SELECT DB_NAME() DBNAME, GETDATE() DATE in all databases, outputs warnings, errors and detailed error messages
  to specified files. After completing the generation of CSV output file will call Excel and open the target file
```  
ForEach-DB -server vm-pc-sql02 -out output.csv -format CSV -OpenExcel -Warn Warnings.log -Err Errors.log -Query "SELECT DB_NAME() DBNAME, GETDATE() DATE" -Deterror DetError.json
```
  This command binds a variable in the query body passed using the BindVariables parameter
```  
ForEach-DB -server vm-pc-sql02 -out output.csv -format CSV -Query "SELECT DB_NAME() DBNAME, `$(A) AS A" -BindVars @("A='1'")
```
  This command inserts the values read from all data sources into target table TEST_TBL contained in database TESTDB
```  
ForEach-DB vm-pc-sql02 "select db_name() dbname, e.firstname from employee e" -pumpToTable "TEST_TBL" -PumpDB TESTDB
```