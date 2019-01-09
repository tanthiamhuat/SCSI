## Accumulative, Appended on Daily basis for DailyLPCD

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

yesterday <- today()-1

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")

family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                 dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & 
                               status=="ACTIVE" & id_service_point!=601)) # exclude AHL
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type")) 

consumption_last30days_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last30days_servicepoint.fst",as.data.table=TRUE)
Punggol_All <- consumption_last30days_servicepoint[site=="Punggol"]

PunggolSUB <- Punggol_All[meter_type=="SUB"]

PunggolConsumption <- inner_join(PunggolSUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 
 
PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","date_consumption","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(date=date(date_consumption)) %>%
  group_by(service_point_sn,date,num_house_member) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_New <- DailyConsumption %>% dplyr::filter(date==yesterday) %>%
             dplyr::group_by(date) %>%
             dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
             dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD <- rbind(DailyLPCD,DailyLPCD_New)

DailyLPCD_CSV <- DailyLPCD%>% dplyr::select_("date","DailyLPCD")

DailyLPCD_xts <- xts(DailyLPCD$DailyLPCD,order.by=as.Date(DailyLPCD$date))
colnames(DailyLPCD_xts) <- "DailyLPCD"

Updated_DateTime_DailyLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(DailyLPCD,DailyLPCD_xts,Updated_DateTime_DailyLPCD,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")
write.csv(DailyLPCD_CSV,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_DailyLPCD_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)