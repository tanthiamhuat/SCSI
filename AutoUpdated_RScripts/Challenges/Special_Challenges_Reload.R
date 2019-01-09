
special_challenges_new <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/special_challenges.csv")
special_challenges_new$X <- NULL
special_challenges_new$challenge_start_date <- as.Date(special_challenges_new$challenge_start_date)
special_challenges_new$challenge_end_date <- as.Date(special_challenges_new$challenge_end_date)
special_challenges_new$created_date <- as.Date(special_challenges_new$created_date)
special_challenges_new$updated_date <- as.Date(special_challenges_new$updated_date)

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

dbWriteTable(mydb, "special_challenges", special_challenges_new, append=FALSE, row.names=F, overwrite=TRUE)


all_special_challenges <- as.data.frame(tbl(con,"special_challenges")) 
special_challenges_test <- all_special_challenges %>% dplyr::group_by(family_id) %>%
                           dplyr::filter(results=="AVAILABLE") %>%
                           dplyr::mutate(Count=n()) %>%
                           dplyr::filter(Count !=1)




leak_alarm_new <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/leak_alarm.csv")
leak_alarm_new$X <- NULL
leak_alarm_new$start_date <- as.Date(leak_alarm_new$start_date)
leak_alarm_new$end_date <- as.Date(leak_alarm_new$end_date)

dbWriteTable(mydb, "leak_alarm", leak_alarm_new, append=FALSE, row.names=F, overwrite=TRUE)
