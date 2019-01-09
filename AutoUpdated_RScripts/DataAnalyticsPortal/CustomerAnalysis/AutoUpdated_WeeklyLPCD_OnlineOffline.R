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
thisYear <- year(today())
lastYear <- thisYear-1

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[,1:12]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

Punggol_All <- rbind(Punggol_2017,Punggol_thisyear) %>% dplyr::filter(Date>="2017-06-10")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 
DailyConsumption <- DailyConsumption %>% dplyr::filter(date <= yesterday)

weeknumber <- as.numeric(strftime(DailyConsumption$date,format="%W")) %>% as.data.frame()
colnames(weeknumber) <- "week_number"
weeknumber <- weeknumber %>% dplyr::mutate(week_number=ifelse(week_number<10,paste(0,weeknumber$week_number,sep=""),week_number))

yearnumber <- DailyConsumption$Year %>% as.data.frame()
colnames(yearnumber) <- "yearnumber"
yearweek <- cbind(yearnumber,weeknumber) %>% dplyr::mutate(yearweek=paste(yearnumber,week_number,sep="_")) %>% dplyr::select_("yearweek")

DailyConsumption_yearweek <- cbind(as.data.frame(DailyConsumption),yearweek) 
WeeklyConsumption <- DailyConsumption_yearweek %>%
  dplyr::group_by(service_point_sn,num_house_member,yearweek) %>%
  dplyr::summarise(WeeklyConsumption=sum(DailyConsumption,na.rm=TRUE))

OnlineCustomers <- family %>% dplyr::filter(online_status=="ACTIVE")
OfflineCustomers <- family %>% dplyr::filter(online_status=="INACTIVE")

OnlineCustomers_ServicePoint <- inner_join(OnlineCustomers,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","num_house_member") %>% collect()
OfflineCustomers_ServicePoint <- inner_join(OfflineCustomers,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","num_house_member") %>% collect()

WeeklyConsumption_Online <- WeeklyConsumption %>% dplyr::filter(service_point_sn %in% OnlineCustomers_ServicePoint$service_point_sn)
WeeklyConsumption_Offline <- WeeklyConsumption %>% dplyr::filter(service_point_sn %in% OfflineCustomers_ServicePoint$service_point_sn)

WeeklyLPCD_Online <- WeeklyConsumption_Online %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD_Online=round(TotalWeeklyConsumption/(TotalHH*7))) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD_Online")
WeeklyLPCD_Online <- WeeklyLPCD_Online[2:(nrow(WeeklyLPCD_Online)),]

WeeklyLPCD_Offline <- WeeklyConsumption_Offline %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD_Offline=round(TotalWeeklyConsumption/(TotalHH*7))) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD_Offline")
WeeklyLPCD_Offline <- WeeklyLPCD_Offline[2:(nrow(WeeklyLPCD_Offline)),]

WeeklyLPCD_OnOffline <- inner_join(WeeklyLPCD_Online,WeeklyLPCD_Offline,by=c("Year","Week")) %>% as.data.frame()
WeeklyLPCD_OnOffline <- WeeklyLPCD_OnOffline %>% dplyr::mutate(YearWeek=paste(Year,Week,sep="_"))

write.csv2(WeeklyLPCD_OnOffline,row.names=FALSE,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCDOnlineOffline.csv")

Updated_DateTime_WeeklyLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(WeeklyLPCD_OnOffline,Updated_DateTime_WeeklyLPCD,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD_OnOffline.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_WeeklyLPCD_OnlineOffline successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
