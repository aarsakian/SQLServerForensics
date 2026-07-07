# Microsoft SQL Server Forensics tool.
 
## Table of Contents ##

***[Description](#description)***  
***[Technical Details](#technical-details)***  
[Dependencies](#dependencies)  
***[Licensing](#licensing)***  
***[Usage Instructions](#usage-instructions)***  
[Input Options](#input-options)   
[Output Options](#output-options)  
[Export Options](#export-options)  
[Log Options](#log-options)  
[Misc options](#misc-options)  
***[Examples](#examples)***  
[Table Operations](#table-operations)  
[Page Internals Inspection](#page-internals-inspection)   
[Transaction Log internals Inspection](#transaction-log-internals-inspection)    
[Additional Category Examples](#additional-category-examples)  
[Full Archive Backup (BAK)](#full-archive-backup-bak)  
[Working With Evidence](#working-with-evidence)  



## Description ##
This tool is designed for digital forensics examiners, incident responders, and database engineers who need direct, efficient access to the internal structures of SQL Server databases without relying on SQL Server itself. It provides the DFIR community with a fast, convenient, and reliable way to inspect database contents—whether working in a controlled lab environment or conducting on‑site field investigations.

***Who is this For?***

Professionals involved in financial-fraud investigations, or cases involving leaked company data will find this tool especially valuable. It enables them to examine the contents of an SQL Server database, uncover details not visible to regular users, recover deleted records through carving and correlate transaction-log-activity with table records. 


## Technical Details ##

This tool performs *read-only* operations on SQL Server database files. However, users must understand that ***data loss might occur or corruption may occur*** if low-level access methods are used on live systems.
The low level access option works exclusively on a ***local copy*** of your database. Copying is being performed at a cluster level using the lowest-level userspace  Win32 API available ensuring minimal interference with the operating system's running processes.

The table below outlines the implications of accessing your database on a running server. 

| Access Method| Implication | Server is running |
| ----------- | ----------- | --- |
| stopservice | Certain data loss | Server will be stopped | 
| physical disk | Unlikely |  Performance impact  |

 The tool can parse tables ***directly*** from MDF files and TAPE archives (full backup files).  When reading BAK files, their contents are first extracted into local MDF files (default location is MDF folder) before being processed. Log processing from BAK files  is not currently supported. Page processing is ***experimental*** and it does not rely on  backup block format when parsing but rather using raw page structure.
 
 Carving table records is supported. Log parsing and carving is available as well when an LDF  file is provided.  The tool attempts to correlate existing table records with their corresponding log entries to reconstruct changes and recover additional context.

Advanced users can inspect the internals of a SQL Server database, including the log file. Inspection is supported as multiple levels of granularity: page level, row level, and column level, log actions.
 

Responders who require immediate access to a SQL Server database, may either stop the SQL Server service  (not recommended due to irreversible data consequences) or you use low level physical-disk access  *without* stopping the server. Both methods require ***Admin*** privileges. 

When low level access is used, MDF file and LDF files are copied *locally*, before  processing (defaut location is MDF folder). Responders must understand how logging works behind the scenes to avoid misinterpreting missing data or incomplete data—for example, committed rows that have not yet been written to the data file (MDF file) and therefore do not appear in the table.


 Digital forensics examiners can access SQL database by reading mdf, ldf, bak files directly from disk images. Supported image formats include dd, EWF (E01), VMDK (including sparse variants), and VHDX virtual hard disk images, including differencing VHDX files. No third-party dependencies are required to read expert witness format files (E01) or the NTFS file system. Both capabilities are provided by external libraries developed by the same author. Any files discovered are copied locally (default location MDF folder) before processing. 


A GUI is under development, and a protobuf-based gRPC service is also available for remote processing and export workflows via the `-rpc` flag. It will be offered under a paid license. 

Additional features will be introduced over time, but no specific time release schedule can be provided. 

The development of this tool is based on personal research and published academic work. 

### Dependencies ###  
This tool requires [EWF library](https://github.com/aarsakian/EWF_Reader) for parsing E01 images, [MTF library](https://github.com/aarsakian/MTF_Reader) for handling TAPE (bak) files, and [VMDK library](https://github.com/aarsakian/VMDK_Reader) for reading VMware disk images, [NTFS library](https://github.com/aarsakian/FileSystemForensics) for accessing NTFS file systems without relying on Windows file system APIs. The afore mentioned libraries are developed by the same author. In addition, it requires selected modules of the [x](https://go.dev/wiki/X-Repositories) for interacting with Windows APIs and the [Google's grpc](https://pkg.go.dev/google.golang.org/grpc) libraries for using protobufs.

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
        path to bak file (TAPE format)

  -evidence string
        path to image file
        
  -vmdk string
        path to vmdk file (Sparse formats are supported)

  -physicaldrive int
        select the physical disk number to look for MDF file (requires admin rights!) (default -1)

  -partition int
        select the partition number to look for MDF files (requires admin rights!) (default -1)

  -filenames string
        select mdf files to filter use comma for each file (to be used with evidence)

  -sourcedir string 
        process all mdf and ldf files found in source directory

  -location string
        the path to export MDF/LDF files (default "MDF")
      
### Processing options

  -processtables
        process tables

  -carve
        Carve data records and try to interpret

  -bak
        parse bak files found in images

  -from int
        select page id to start parsing (default 0)

  -to int
        select page id to end parsing (default -1)

  -pages string
        select pages to parse (use comma for each page id, other pages will be ignored)

  -type string
        filter by page type (IAM, GAM, SGAM, PFS, DATA)

  -filterlop string
        filter log records per lop type (values: insert, begin, commit, begin_ckpt, end_ckpt, any)

### Output options

#### Database Information

  -showdbinfo
        show database information parsed from fileheader (page 0)

#### Page related

  -showheader
        show page header

  -showpages string
        select specific pages to show info (use comma for each page id)

  -showpagestats
        show page statistics (SGAM, GAM, PFS, IAM for selected pages)

  -type string
        filter by page type (IAM, GAM, SGAM, PFS, DATA)

  -showgam
        show GAM extents for each page

  -showsgam
        show SGAM extents for each page

  -showiam
        show IAM extents for each page

  -showpfs
        show pfs page allocation

  -showdiffmap
        show differential map for each page

  -showbcm
        show bulk change map

  -showslots
        show page slots

  -showdatacols
        show data cols for each data row

  -showindex
        show index contents

  -walklsn string
        follow log records LSN (values: any, backward, forward)
        requires -filterlop

  -walkpagelsn string
        follow log records associated with page (values: any, backward, forward)
        requires -filterlop

  -sortbylsn string
        sort pages (values: all, allocunit)

#### Log related Options

  -showldf
        show vlf, log blocks and records of ldf files

  -showtableldf
        show table log record info (must be used with -tables)

  -walklsn string
        follow log records LSN (values: any, backward, forward)
        requires -filterlop

  -walkpagelsn string
        follow log records associated with page (values: any, backward, forward)
        requires -filterlop

#### Table Filtering Options

  -tables string
        select the tables to process (use comma for each table name)

  -tabletype string
        filter tables by type (e.g. 'User Table' for user tables, 'View' for views)

  -tablepages string
        filter rows by pages (use comma)

  -fromrow int
        show only rows starting from this row number (default 0)

  -torow int
        show only the first N rows (default -1 for all)

  -rows string
        use comma to select specific rows (e.g. 1,5,10)

  -colnames string
        the columns to display (use comma for each column name)

#### Table Related Options

  -processtables
        process tables

  -showcontent
        show table contents

  -showschema
        show table schema

  -showtableindex
        show table index contents

  -showtableallocation string
        show pages that the table has been allocated (values: simple, sorted, links)

  -showraw
        show row data for each column in a table in raw (hex) format

  -systemtables string
        show information about system tables (sysschobjs, sysrowsets, syscolpars)

  -usertable string
        get system table info about user table


### Volume and Verification Options

  -verifysignatures
        verify BAK/TAPE file signatures during processing

  -password string
        password for BitLocker-protected volumes

  -recoverykey string
        recovery key for BitLocker-protected volumes

### Export Options

  -export string
        export tables to selected path

  -format string
        select format to export (csv, html, or xlsx)

  -exportschema
        export table schema

  -exportindex
        export table indexes

  -exportblob
        export blobs (will be exported to a folder named blobs under the database name, file extension is blob)
 

 ### Log Options

  -log
        log activity

### Misc options

  -stopservice
        stop MSSQL service (requires admin rights!)

  -rpc uint
        use grpc to communicate, select port from 1024 and upwards

  -profile
        profile memory usage


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

Export table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports*** in ***html***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables  -export MyExports -tables PersonPhone -format html

Export all user table contents  of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports*** in ***csv***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tabletype 'User Table' -export MyExports -format csv

Export all ***Customer and Address*** table contents including ***index and schema*** information of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports*** in ***html*** 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables "Customer,Address" -export MyExports -format html -exportschema -exportindex


Show table allocation information such as ***Partition IDs, AllocationUnit IDs*** of table ***PersonPhone***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableallocation links -tables PersonPhone

Show table allocation information such as ***Partition IDs, AllocationUnit IDs*** of table ***PersonPhone*** including ***DATA, Index, IAM*** pages sorted by ID
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableallocation sorted -tables PersonPhone

Show table schema of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showschema -tables PersonPhone


Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***  to row ***120*** correlate with log file entries ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")***. When a record is found in the transaction log relevant timestamps are shown. 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -torow 120 -showcontent -showtableldf

Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** , ***carve*** records, correlate with log file entries ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")*** including carved records. 
When a record is found in the transaction log relevant timestamps are shown. 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showtableldf -carve -showcontent

###  Page Internals Inspection 

Show page number and index names of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showtableindex -tables PersonPhone

Show page  information including ***header, slot offsets and possible slack space, data column offsets and contents*** of page 6432 of database file ***AdventureWorks2022.mdf*** 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf  -showheader -showslots -showdatacols -showpages  6432


Show page information including ***index structure FileID, PageID, Key, RowSize*** of index page 11854
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf   -showindex -showpages  11854


Show page information including ***header, relevant log records of any lop type operation, follow log records both directions (backward and forward)***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -showheader -filterlop any -walklsn any -sortbylsn all


Show page ***bit allocation maps (PFS, GAM, SGAM, IAM, DIFFMAP)*** for pages 423, 454 
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -showpages 423,454 -showpagestats

### Transaction Log Internals Inspection 
Show transaction log data changes ***("LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW")*** such as ***Log Block Header Slots, size of block, FirstLSN*** operations for log file ***AdventureWorks2022_log.ldf*** 
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -showldf -filterlop any


### Additional Category Examples

Show basic database metadata from file header (database information category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -showdbinfo

Process a BitLocker-protected volume or image and provide either the password or recovery key so the tool can access the contained database files
> .\MSSQLParser.exe -evidence C:\Evidence\disk.E01 -location MyDBs -password "YourPassword" -processtables -export MyExport -format csv

Show system tables metadata (system tables category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -processtables -systemtables sysschobjs

Get system-table details for a specific user table (user table metadata category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -processtables -usertable PersonPhone

Export blob columns for a table (export blob category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables ProductPhoto -exportblob -export MyExports

Enable file logging while exporting table data (log option category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showcontent -log -export MyExports -format csv




### Full Archive Backup (BAK)
You can apply all operations of database file mdf to bak files, for instance see below:

Export all tables of backup file ***AdventureWorks2022.bak***, mdf produced file will be saved to location ***BackupDB***
> .\MSSQLParser.exe -mtf ..\Shared-mssql\data\AdventureWorks2022.bak -location BackupDB -processtables -export TablesFromBackup -format csv


### Working With Evidence
Export all tables to ***MyExport*** of any database file found in image ***tester-ewf***, database files are exported to ***MyDBs*** (Locating database files is based on extension)
>.\MSSQLParser.exe -evidence C:\Users\User\Downloads\evidence\tester-ewf.E01 -location MyDBs -processtables -export Myexport -format csv
