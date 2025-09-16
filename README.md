# Microsoft SQL Server recovery tool.
 

## Description ##
This tool is designed to perform read-only operations on database files. However, users must understand that ***permanent data loss might occur*** or corruption may occur if low-level access methods are used on active systems.
The low level option works only on a local copy of your database. Copying is being performed at a cluster level using low level OS APIs. 

Below is a table helping you to understand the implications of accessing your database for a running server. 
| Access Method| Implication | Server is running |
| ----------- | ----------- | --- |
| stopservice | Certain data loss | Server will be stopped | 
| physical disk | Unlikely |  Disruptions to the performance  |

 It can parse tables ***directly*** from mdf files and TAPE archives (full backup files). If you opt to read backup (bak) files the contents are saved to mdf files *locally* (default location is MDF folder) before being processed. Carving is also supported it may not work with varying length schemas. Log Parsing is available, if you provide the ldf file. It will attempt to correlate existing table records with the respective log records. Carving log records is also available. 

 For advanced users there are a lot of options to inspect the internals of your database including the log file. Inspection is supported  at a page level, data row level, and  data column level, log action.
 

For responders who want immediate access to the MS SQL database, they can stop service (please there are irreversible consequences in your data, not recommended) or you can use low level access reading directly from physical disk *without* stopping the server. Please note that ***admin*** access priveleges are required for both kind of operations. If you opt for low level access your mdf file and ldf file will be respectively copied *locally*, before being processed (defaut location is MDF folder). The responder needs to know how logging works so as to avoid being suprised of missing data, for instance commited data that is missing from a table. 


For digital forensics examiners they can access SQL database by reading mdf, ldf, bak files directly from images. Supported images are dd, EWF (E01), vmdk. There are no third parties to read expert witness format and NTFS file system. Both functionalities are provided by external libraries developed by the same author. 


A GUI is on the way which will communicate via rpc calls  (protobufs) to the backend. It will offer limited functionality compared to command line usage. 

Many more features will be introduced and testing will be continued.

The development of this tool is based on personal research and published papers. 


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

### Processing options 
 
 
-bak
        parse bak files found in images

-carve
        Carve data records and try to interpret

  -ldf int
        parse hardened (commited) log transactions 1: data changes  2: full changes

-to int
        select page id to end parsing (default -1)
 
-from int
        select page id to start parsing

-location string
        the path to export MDF/LDF files (default "MDF")

-page int
        select a page to start parsing (default -1)
 
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


  -datacols
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
        filter tables by type e.g. 'User Table' for user tables

-showtableallocation string
        show pages that the table has been allocated write 'simple', 'sorted' or 'links' to see the linked page structure

-raw
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
        export table

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
  
 




