rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

#####
### Special challenges
#####
all_special_challenges <- as.data.frame(tbl(con,"special_challenges")) 
setwd("/srv/shiny-server/DataAnalyticsPortal/data")
write.csv(all_special_challenges,file="special_challenges_AWS.csv")

## clear special_challenge table and load special_challenges.GDC.csv
dbSendQuery(mydb, "delete from special_challenges")

special_challenges_GDC <- read.csv("special_challenge_GDC.csv",header = TRUE, row.names=1,sep=",")

dbWriteTable(mydb, "special_challenges", special_challenges_GDC, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)