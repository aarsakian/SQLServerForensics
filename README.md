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
This tool requires [EWF library](https://github.com/aarsakian/EWF_Reader) for parsing E01 images, [MTF library](https://github.com/aarsakian/MTF_Reader) for handling TAPE (bak) files, [VMDK library](https://github.com/aarsakian/VMDK_Reader) for reading VMware disk images, [VHDX library](https://github.com/aarsakian/VHDX_Reader) for reading Microsoft Hyper-V virtual hard disk images, and [NTFS library](https://github.com/aarsakian/FileSystemForensics) for accessing NTFS file systems without relying on Windows file system APIs. The afore mentioned libraries are developed by the same author. In addition, it requires selected modules of the [x](https://go.dev/wiki/X-Repositories) for interacting with Windows APIs and the [Google's grpc](https://pkg.go.dev/google.golang.org/grpc) libraries for using protobufs.

## LICENSING ## 
Read license file.

## Usage Instructions 
Usage instructions have been grouped so as to help the user. 

### Quick Start

Run the tool directly from source:

> go run . -help

Build a binary and then run it:

> go build -o MSSQLParser.exe
> .\MSSQLParser.exe -help

### Input Options

  -db string
        absolute path to the MDF file

  -base string
        absolute path to the base MDF file for comparison (requires -compare)

  -compare
        compare processed MDF files with the base MDF

  -ldb string
        absolute path to the LDF file

  -mtf string
        path to a BAK/TAPE file

  -evidence string
        path to an EWF/VHDX/raw image file; exported MDF/LDF files discovered in the image are processed automatically

  -physicaldrive int
        select the physical disk number to look for MDF files (requires admin rights!) (default -1)

  -partition int
        select the partition number to look for MDF files (requires admin rights!) (default -1)

  -filenames string
        select MDF files by filename; comma-separated values are allowed

  -srcdir string
        process all MDF and LDF files found under the given directory

  -location string
        export location for recovered MDF/LDF files (default "MDF")

### Processing Options

  -processtables
        process table metadata and records

  -carve
        carve data records and try to interpret them

  -bak
        parse BAK files found in images

  -from int
        select the page ID to start parsing (default 0)

  -to int
        select the page ID to end parsing (default -1)

  -pages string
        select pages to parse; use comma-separated page IDs and inclusive ranges (e.g. 1,3-5)

  -pagetype string
        filter by page type (IAM, GAM, SGAM, PFS, DATA)

  -filterlop string
        filter log records per LOP type (insert, begin, commit, begin_ckpt, end_ckpt, any)

  -sortbylsn string
        sort pages by LSN (all or allocunit)

  -systempages string
        filter pages used by system tables (sysschobjs, sysrowsets, syscolpars)

### Output Options

#### Database Information

  -showdbinfo
        show database information parsed from file header (page 0)

#### Page Inspection

  -showheader
        show page header

  -showpages string
        select pages to show; use comma-separated page IDs and inclusive ranges (e.g. 1,3-5)

  -showpageldf
        show LDF records for pages selected with -showpages (requires an LDF source)

  -showpagestats
        show page statistics for SGAM, GAM, PFS, and IAM pages

  -showgam
        show GAM extents for each page

  -showsgam
        show SGAM extents for each page

  -showiam
        show IAM extents for each page

  -showpfs
        show PFS page allocation

  -showdiffmap
        show differential map for each page

  -showbcm
        show bulk change map

  -showslots
        show page slots

  -showdatacols
        show data columns for each data row

  -showindex
        show index contents

  -showldf
        show VLFs, log blocks, and log records from LDF files

  -showtableldf
        show table log record info (must be used with -tables)

  -walklsn string
        follow log records by LSN (values: any, backward, forward); requires -filterlop

  -walkpagelsn string
        follow log records associated with a page (values: any, backward, forward); requires -filterlop

#### Table Filtering and Display

  -tables string
        select the tables to process; comma-separated names

  -tabletype string
        filter tables by type, for example 'User Table' or 'View'

  -tablepages string
        filter rows by page; comma-separated IDs

  -fromrow int
        show/export rows starting at this row number (default 0)

  -torow int
        show/export rows through this row number (default -1 for all)

  -rows string
        select specific rows using a comma-separated list (for example 1,5,10)

  -colnames string
        the columns to display; comma-separated names

  -showcontent
        show table contents

  -rowpageinfo
        show page and slot information with -showcontent, or include it in exports
        requires -showcontent for display or -export and -format for export

  -showschema
        show table schema

  -showtableindex
        show table index contents

  -showtableallocation string
        show allocated pages for a table (values: simple, sorted, links)

  -showraw
        show row data for each column in raw (hex) format

  -systeminfo
        get system table info about user tables

### Volume and Verification Options

  -verifysignatures
        verify BAK/TAPE file signatures during processing

  -password string
        password for BitLocker-protected volumes

  -recoverykey string
        recovery key for BitLocker-protected volumes

### Export Options

  -export string
        export tables to the selected path

  -format string
        select export format (csv, html, xlsx)

  -exportschema
        export table schema

  -exportindex
        export table indexes

  -rowpageinfo
        add each row's page and slot information to CSV/HTML/XLSX exports;
        requires -export and -format (see table display options for pathless display)

  -rowloginfo
        show log-correlation information with -showcontent and include it in exports;
        requires -showcontent or -export

  -exportblob
        export blobs into a folder named blobs under the database name

### Log Options

  -log string
        log activity at the minimum level (info|warning|error)

        If the flag is provided without a value, it defaults to info.

### Misc Options

  -stopservice
        stop the SQL Server service (requires admin rights!)

  -rpc uint
        use gRPC to communicate on the selected port (1024 and upwards)

  -profile
        profile memory usage

### Supported Data Sources

The current CLI supports reading MDF, LDF, and BAK/TAPE files directly, as well as evidence containers and physical disk sources:

- MDF/LDF files
- Full backup files (BAK/TAPE via -mtf or -bak)
- Raw images, EWF, VMDK, and VHDX evidence containers via -evidence
- Physical disks via -physicaldrive
- Directory scans via -srcdir

> Note: the implementation validates compatibility between flags before processing, so some options are only valid when a matching data source or analysis mode is enabled.

### Notes

This project is a Go CLI. The expected entry point is the main package, and it can be run with `go run .` or with a built executable such as `.\MSSQLParser.exe`.
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

  -rowpageinfo
        add each row's page and slot information to CSV/HTML/XLSX exports;
        requires -export and -format (see table display options for pathless display)

  -rowloginfo
        show transaction-log correlation information with -showcontent and include it in exports;
        requires -showcontent or -export

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

Show table contents with each row's page and slot information, without exporting
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -processtables -showcontent -rowpageinfo -tables PersonPhone

Show table contents with each row's transaction-log correlation information, without exporting
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showcontent -rowloginfo -tables PersonPhone

Show table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** from row ***99*** to row ***120*** as pulled from the data pages
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -fromrow 99  -torow 120 -showcontent

Show  table contents of table ***PersonPhone*** in raw (hex values) format of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -showcontent  -tables PersonPhone  -showraw

Show  table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** stored only at page ***17161***
>.\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showcontent -tablepages 17161

Export table contents of table ***PersonPhone*** of database file ***AdventureWorks2022.mdf*** and log file ***AdventureWorks2022_log.ldf*** to folder ***Myexports*** in ***html***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables  -export MyExports -tables PersonPhone -format html

Export table contents with row-level page and log correlation details to ***Myexports*** in ***html***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -export MyExports -format html -rowpageinfo -rowloginfo

Export table contents with page-level metadata for each row to ***Myexports*** in ***html***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -export MyExports -format html -rowpageinfo

Export table contents with transaction-log correlation details to ***Myexports*** in ***html***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -export MyExports -format html -rowloginfo

Export blob columns for a table to ***Myexports***
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables ProductPhoto -export MyExports -exportblob

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

Show metadata from the SQL Server system tables for a database
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -processtables -systeminfo

Get system-table details for a specific user table using the table-processing pipeline
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -processtables -tables PersonPhone -systeminfo

Export blob columns for a table (export blob category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables ProductPhoto -exportblob -export MyExports

Enable file logging while exporting table data at the warning level (log option category)
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showcontent -log warning -export MyExports -format csv

Use the bare flag to enable logging with the default minimum level of info
> .\MSSQLParser.exe -db ..\Shared-mssql\data\AdventureWorks2022.mdf -ldb ..\Shared-mssql\data\AdventureWorks2022_log.ldf -processtables -tables PersonPhone -showcontent -log -export MyExports -format csv

### Full Archive Backup (BAK)
You can apply all operations of database file mdf to bak files, for instance see below:

Export all tables of backup file ***AdventureWorks2022.bak***, mdf produced file will be saved to location ***BackupDB***
> .\MSSQLParser.exe -mtf ..\Shared-mssql\data\AdventureWorks2022.bak -location BackupDB -processtables -export TablesFromBackup -format csv


### Working With Evidence
Export all tables to ***MyExport*** of any database file found in image ***tester-ewf***, database files are exported to ***MyDBs*** (Locating database files is based on extension)
>.\MSSQLParser.exe -evidence C:\Users\User\Downloads\evidence\tester-ewf.E01 -location MyDBs -processtables -export Myexport -format csv

### Comparing a Database from an AVHDX Image

Compare the MDF found in an AVHDX differencing image with a base MDF. The AVHDX is treated as the evidence image; discovered database files are exported to ***RecoveredDB*** before comparison.
> .\MSSQLParser.exe -evidence C:\Evidence\sql-checkpoint.avhdx -location RecoveredDB -base C:\Evidence\baseline\AdventureWorks2022.mdf -compare -showcomparisons

To select one database when the AVHDX contains multiple MDF files, add `-filenames` with the MDF filename or a comma-separated list of filenames.
> .\MSSQLParser.exe -evidence C:\Evidence\sql-checkpoint.avhdx -filenames AdventureWorks2022.mdf -location RecoveredDB -base C:\Evidence\baseline\AdventureWorks2022.mdf -compare -showcomparisons
