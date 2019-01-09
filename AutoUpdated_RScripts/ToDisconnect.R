## Error in postgresqlNewConnection(drv, ...) : 
## RS-DBI driver: (/5~�cannot allocate a new connection -- maximum of 16 connections already opened)
## dbClearResult(dbListResults(con)[[1]])

cons <- dbListConnections(PostgreSQL())
for(con in cons) 
  dbDisconnect(con)