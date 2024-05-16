# Report Pure Storage FlashArray Backup Status

Use the Pure Storage REST API via the Python client to collect information on backup health and compile into a report.

## THIS PROJECT IS INCOMPLETE
### Current Function:
* Establish REST session with single array
* Collect replica pod status
* Convert API output to pandas dataframe
* Re-order & reformat the contents of the replica pod status dataframe
* Convert pandas dataframe to HTML table
** Add in-line CSS styling
* Output HTML to file