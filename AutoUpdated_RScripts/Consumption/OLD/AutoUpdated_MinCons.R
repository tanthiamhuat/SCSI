rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

#testline

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,lubridate,data.table,mailR)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

Punggol_last6months <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

min_cons <- Punggol_last6months %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD"))) %>%
  dplyr::mutate(date=date(adjusted_date)) %>%
  select(service_point_sn,adjusted_consumption,adjusted_date,date) %>%
  group_by(service_point_sn,date) %>%
  dplyr::summarise(MinCons=min(adjusted_consumption,na.rm = TRUE))

# remove those rows with Inf
min_cons <- min_cons[-which(min_cons$MinCons=="Inf"),]

min_cons$id <- as.integer(rownames(min_cons))
min_cons <- min_cons %>% dplyr::select_("id","service_point_sn","date","MinCons") %>% as.data.frame()

dbSendQuery(mydb, "delete from min_cons")
dbWriteTable(mydb, "min_cons", min_cons, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_MinCons successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

