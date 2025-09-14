# Microsoft SQL Server recovery tool.
 
## DISCLAIMER: ##
This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability—whether in an action of contract, tort, or otherwise—arising from, out of, or in connection with the software or the use or other dealings in the software. Users are solely responsible for evaluating the software’s suitability and safety for their systems. Use at your own risk.

## Description ##
Although this tool have been developed in a way that only read operations are allowed to a database, understand that ***permanent data loss might occur*** if you opt for the low level read methods in order to access an active database. The low level option works only on a local copy of your database. Copying is being performed at a cluster level using low level OS apis. 

Below is a table helping you to understand the implications of accessing your database for a running server. 
| Access Method| Implication | Server is running |
| ----------- | ----------- | --- |
| stopservice | Certain data loss | Server will be stopped | 
| physical disk | Unlikely |  Disruptions to the performance  |

 It can parse tables ***directly*** from mdf files and TAPE archives (full backup files). If you opt to read backup (bak) files the contents are saved to mdf files *locally* (default location is MDF folder) before being processed. Carving is also supported it may not work with varying length data. Log Parsing is available if you provide the ldf file and it will attempt to correlate existing table records with the respective log records. Carving log records is also available. 

 For advanced users there are a lot of options to inspect the internals of your database including the log file. Inspection is supported  at a page level, data row level, and  data column level.
 

For responders that we want immediate access to the MS SQL database they can stop service (please there are irreversible consequences in your data, not recommended) or you can use low level access reading directly from physical disk *without* stopping the server. Please note that admin access priveleges are required for both kind of operations. If you opt for low level access your mdf file and ldf file will be copied *locally* and then they will be processed (defaut location is MDF folder). The responder needs to know how logging works so as to avoid being suprised of missing data e.g. commited data that is missing from a table. 


For digital forensics examiners they can access database by reading mdf, ldf, bak files directly from images. Supported images are dd, EWF (E01), vmdk. 


A GUI is on the way which will communicate via rpc calls to the backend. 


## Usage Instructions 

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
  
 




