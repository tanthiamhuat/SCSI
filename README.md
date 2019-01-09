# Data Analytics Portal
Data Analytics Portal (DAP) is developed by Suez mainly to help monitor water consumption patterns and identify anomalies 
in the water network towards water conservation and improvement of the operations.

The data comes from Automated Meter Readings in selected blocks from 3 estates in Singapore: Punggol, Tuas and Yuhua.

DataAnalyticsPortal contains different directories and files.

/ AutoUpdated_RScripts subdirectory contains R Scripts which are automatically run using Cron.

/ data subdirectory contains the input (.csv, .xlsx) and output files (RData, fst)

/ Output subdirectory contains printscreens of DAP

/ source subdirectory contains source R script for the UI

/ www subdirectory contains JavaScript files and common icons.

ui.R is the file where the different parts of the application's frontend (that is, what the end users see) is defined. 

server.R , on the contrary, is the backend or the engine of the application, that is, where the data is processed.


