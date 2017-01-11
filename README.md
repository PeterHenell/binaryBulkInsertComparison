# binaryBulkInsertComparison
Comparing bulk inserts done as a binary field vs 50 float fields

Uses dll from https://github.com/PeterHenell/IDataReaderMock which allow me to use any collection of objects as a IDataReader.

## How to run
 
 * Run the sql script to create the tables
 * Open up the solution and make sure that it is compiling, you may need to fix the reference to the IDataReaderMock which is included in the thirdpartybinaries folder.
 * Alter the connectionstring in the GetConnectionString function to point to your database server of choice.
 * Run the application. It will exit after about 1 minute.
 
## What it did while running

* For both 50 columns and then 1 binary column:
** Continuously bulk inserted batches of 2000 rows until 30 seconds had passed.


## But how can there be such a big difference between the two methods?

* ColumnConsumer is probably falling behind due to all the reflection being used by the IDataReaderMock. Needs more investigation to clarify.