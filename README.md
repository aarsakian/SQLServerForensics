# Microsoft SQL Server Forensics tool.
 

## Description ##
This tool is designed for digital forensics examiners, incident responders, and database engineers who need direct, efficient access to the internal structures of SQL Server databases without relying on SQL Server itself. It provides the DFIR community with a fast, convenient, and reliable way to inspect database contents—whether working in a controlled lab environment or conducting on‑site field investigations.

***Who is this For?***

Professionals involved in financial-fraud investigations, or cases involving leaked company data will find this tool especially valuable. It enables them to examine the contents of an SQL Server database, uncover details not visible to regular users, recover deleted records through carving and correlate transaction-log-activity with table records. 


### Technical details ###

This tool performs *read-only* operations on SQL Server database files. However, users must understand that ***data loss might occur or corruption may occur*** if low-level access methods are used on live systems.
The low level access option works exclusively on a ***local copy*** of your database. Copying is being performed at a cluster level using the lowest-level userspace  Win32 API available ensuring minimal interference with the operating system's running processes.

The table below outlines the implications of accessing your database on a running server. 
| Access Method| Implication | Server is running |
| ----------- | ----------- | --- |
| stopservice | Certain data loss | Server will be stopped | 
| physical disk | Unlikely |  Performance impact  |

 The tool can parse tables ***directly*** from MDF files and TAPE archives (full backup files).  When reading BAK files, their contents are first extracted into local MDF files (default location is MDF folder) before being processed. Log processing from BAK files  is not currently supported. 
 
 Carving table records is supported. Log parsing and carving is available as well when an LDF  file is provided.  The tool attempts to correlate existing table records with their corresponding log entries to reconstruct changes and recover additional context.

Advanced users can inspect the internals of a SQL Server database, including the log file. Inspection is supported as multiple levels of granularity: page level, row level, and column level, log actions.
 

Responders who require immediate access to a SQL Server database, may either stop the SQL Server service  (not recommended due to irreversible data consequences) or you use low level physical-disk access  *without* stopping the server. Both methods require ***Admin*** privileges. 

When low level access is used, MDF file and LDF files are copied *locally*, before  processing (defaut location is MDF folder). Responders must understand how logging works behind the scenes to avoid misinterpreting missing data or incomplete data—for example, committed rows that have not yet been written to the data file (MDF file) and therefore do not appear in the table.


 Digital forensics examiners can access SQL database by reading mdf, ldf, bak files directly from images. Supported images include dd, EWF (E01), vmdk (including sparce variant). No third parties depedencies are required to read expert witness format files (E01) or the NTFS file system. Both capabilities are provided by external libraries developed by the same author. Any files discovered are copied locally (default location MDF folder) before processing. 


A GUI is under development, which  communicates via rpc calls (protobufs) to the backend. It will be offered under a paid license. 

Additional features will be introduced over time, but no specific time release schedule can be provided. 

The development of this tool is based on personal research and published academic work. 

## LICENSING ## 
Read license file.

## Usage Instructions 
Usage instructions have been grouped so as to help the user. 

### Input Options

  -db string
        absolute path to the MDF file

-ldb string
        absolute path to the LDF file

-mtf string
        path to bak file (TAPE format) (log pages are not processed to be changed in the future)

-evidence string
        path to image file
        
  -vmdk string
        path to vmdk file (Sparse formats are supported)

  -physicaldrive int
        select the physical disk number to look for MDF file (requires admin rights!) (default -1)

 -filenames string
        select mdf files to filter use comma for each file (to be used with evidence)

### Processing options 
 
 
-bak
        parse bak files found in images

-carve
        Carve data records and try to interpret

-to int
        select page id to end parsing (default -1)
 
-from int
        select page id to start parsing

-location string
        the path to export MDF/LDF files (default "MDF")

-pages string
       select pages to parse (use comma for each page id)
 
-processtables
        process tables

  -filterlop string
        filter log records per lop type values are insert|begin|commit|any

### Output options
 
#### page related 
 -showheader
        show page header

  -showgam
        show GAM extents for each page

  -showsgam
        show SGAM extents for each page

  -showiam
        show IAM extents for each page

  -showpfs
        show pfs page allocation

   -showpagestats
        show page statistics parses sgam gam and pfm pages

  -showslots
        show page slots


  -showdatacols
        show data cols for each data row

-type string
        filter by page type IAM, GAM, SGAM, PFS, DATA


 -showindex
        show index contents


#### Log related Options 
  -showldf
        show vlf, log blocks and records of ldf files

 


 #### Table filtering options

   -tables string
        select the tables to process (use comma for each table name)

 -fromrow int
        show only the last rows (Default is all)

-torow int
        show only the first rows (Default is all) (default -1)

-colnames string
        the columns to display use comma for each column name
        
-showcontent
        show table contents

-rows string
        use comma to select rows




 #### Table related Options

   -systemtables string
        show information about system tables sysschobjs sysrowsets syscolpars
 
  -tabletype string
        filter tables by type e.g. 'User Table' for user tables 'View' for views

-showtableallocation string
        show pages that the table has been allocated write 'simple', 'sorted' or 'links' to see the linked page structure

-showraw
        show row data for each column in a table

-showtableindex
        show table index contents

  -showtableldf
        show table log record info (must be used with table)

   -showschema
        show table schema

 -tablepages string
        filter rows by pages (use comma)



  -usertable string
        get system table info about user table


### Export Options
 -export string
        export tables to selected path

  -exportImages
        export images saved as blob


  -format string
        select format to export (csv) (default "csv")
 

 ### Log Options

  -log
        log activity


### Misc options
 


  -rpc uint
        communicate via grpc to selected port (from 1024 and upwards)

  -stopservice
        stop MSSQL service (requires admin rights!)
  
 


## Examples 

### Table Operations

Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables  -showcontent  -tables PersonPhone


Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** from row ***99*** to row ***120*** as pulled from the data pages
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -fromrow 99  -torow 120 -showcontent

Show  table contents of table ***PersonPhone*** in raw (hex values) format of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showcontent  -tables PersonPhone  -showraw

Show  table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** stored only at page ***17161***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showcontent -tablepages 17161

Export table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables  -export MyExports -tables PersonPhone

Export all user table contents  of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tabletype 'User Table' -export MyExports

Show table allocation information such as ***Partition IDs, AllocationUnit IDs*** of table ***PersonPhone***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableallocation links -tables PersonPhone

Show table allocation information such as ***Partition IDs, AllocationUnit IDs*** of table ***PersonPhone*** including ***DATA, Index, IAM*** pages sorted by ID
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableallocation links -tables PersonPhone

Show table schema of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showschema -tables PersonPhone


Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***  to row ***120*** correlate with log file entries ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")***. When a record is found in the transaction log relevant timestamps are shown. 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone   -torow 120 -showcontent -ldf 1 -showtableldf

Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** ,  ***carve*** records, correlate with log file entries ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")*** including carved records. 
When a record is found in the transaction log relevant timestamps are shown. 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -ldf 1 -showtableldf -carve -showcontent
###  Page internals inspection 

Show page number and index names of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableindex -tables PersonPhone

Show page  information including ***header, slot offsets and possible slack space, data column offsets and contents*** of page 6432 of database file ***AdventureWorks2022.mdf*** 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf  -showheader -showslots -showdatacols -pages  6432


Show page information including ***index structure FileID, PageID, Key, RowSize*** of index page 11854
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf   -showindex -pages  11854




### Transaction Log internals inspection 
Show transaction log data changes ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")*** such as ***Log Block Header Slots,  size of block, FirstLSN*** operations for log file ***AdventureWorks2022_log.ldf*** 
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf  -showldf -ldf 1


### Full archive backup (BAK)
You can apply all operations of database file mdf to bak files, for instance see below:

Export all tables of backup file ***AdventureWorks2022.bak***, mdf produced file will be saved to location ***BackupDB***
> .\MSSQLParser.exe -mtf ..\Shared-mssql\data\AdventureWorks2022.bak -location BackupDB -processtables -export TablesFromBackup


### Working evidence files 
Export all tables to ***MyExport*** of any database file found in image ***tester-ewf***, database files are exported to ***MyDBs*** (Locating database files is based on extension)
>.\MSSQLParser.exe -evidence C:\Users\User\Downloads\evidence\tester-ewf.E01 -location
 MyDBs -processtables -export Myexport