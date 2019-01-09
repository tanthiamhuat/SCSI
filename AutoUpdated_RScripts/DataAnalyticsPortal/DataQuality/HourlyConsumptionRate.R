rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,fst)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

last90days <- today()-90
today <- today()

consumption_last6months_servicepoint <- read.fst(paste0(path,'data/DT/consumption_last6months_servicepoint.fst'),as.data.table = TRUE)
consumption_last6months_servicepoint_PG <- consumption_last6months_servicepoint %>% dplyr::filter(site=="Punggol")
consumption_last6months_servicepoint_YH <- consumption_last6months_servicepoint %>% dplyr::filter(site=="Yuhua")

total_PG <- length(unique(consumption_last6months_servicepoint_PG$service_point_sn))

HourlyConsumptionRate_last90days <- consumption_last6months_servicepoint_PG %>%
  dplyr::filter(date(date_consumption)>=last90days & date(date_consumption) < today) %>% 
  dplyr::mutate(date=date(date_consumption),hour=hour(date_consumption)) %>%
  dplyr::group_by(date,hour) %>%
  dplyr::summarise(Count=n(),ConsumptionRate=round(Count/total_PG*100)) %>%
  dplyr::mutate(hour=ifelse(hour<10,paste("0",hour,sep=""),hour)) %>%
  dplyr::mutate(DateTime=as.POSIXct(paste(paste(date,hour,sep=" "),":00:00",sep=""))) %>% 
  dplyr::select_("DateTime","ConsumptionRate") %>%
  as.data.frame()

write.csv(HourlyConsumptionRate_last90days,file=paste0(path,'data/HourlyConsumptionRate.csv'),row.names = FALSE)

Updated_DateTime_DataQualityConsumptionRate <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(HourlyConsumptionRate_last90days,Updated_DateTime_DataQualityConsumptionRate,
     file=paste0(path,'data/HourlyConsumptionRate_last90days.RData'))

time_taken <- proc.time() - ptm
ans <- paste("HourlyConsumptionRate successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)