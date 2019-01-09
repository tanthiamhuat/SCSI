rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# Phase_1 – pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 – leak alarm only (from 2016-04-20" to 2017-06-10)
# Phase_3 – leak alarm + app (> 2017-06-10)

# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)    ---------------  (A)
# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  ---------------  (B)

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>%
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE"
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

consumption_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2016.fst",as.data.table = TRUE)
consumption_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017.fst",as.data.table = TRUE)
consumption_previous <- rbind(consumption_2016,consumption_2017)
consumption_previous_servicepoint <- inner_join(consumption_previous,servicepoint,by=c("id_service_point"="id")) %>% 
                                     dplyr::select_("service_point_sn","block","floor","unit","room_type","site","meter_type",
                                                    "interpolated_consumption","adjusted_consumption","date_consumption")

consumption_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",as.data.table = TRUE)
consumption_All <- rbind(consumption_previous_servicepoint,consumption_thisyear)
PunggolSUB <- consumption_All %>% dplyr::filter(meter_type=="SUB" & service_point_sn!="3100660792")

PunggolConsumption <- inner_join(PunggolSUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(date_consumption),date=date(date_consumption),Month=month(date_consumption)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD<- DailyConsumption %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

## -------- Water Savings ------------- ##
# Phase_1 : pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 : leak alarm only (from 2016-04-20" to 2017-06-09)
# Phase_3 : leak alarm + app (>= 2017-06-10)

# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)   
daydifference_1 <- as.numeric(difftime(today()-1,"2016-04-20",units=c("days")))

# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  
daydifference_2 <- as.numeric(difftime(today()-1,"2017-06-10",units=c("days")))

## Gloabl Water Savings.
LPCD_Phase1 <- DailyLPCD %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase1 <- DailyLPCD %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(HHSize_Phase1=mean(TotalHH))  %>% as.numeric()
LPCD_Phase2 <- DailyLPCD %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase2 <- DailyLPCD %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(HHSize_Phase2=mean(TotalHH)) %>% as.numeric()
HHSize_Phase12 <- mean(c(HHSize_Phase1,HHSize_Phase2))
LPCD_Phase3 <- DailyLPCD %>%
  dplyr::filter(date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase3 <- DailyLPCD %>%
  dplyr::filter(date >="2017-06-10") %>% 
  dplyr::summarise(HHSize_Phase3=mean(TotalHH)) %>% as.numeric()
HHSize_Phase23 <- mean(c(HHSize_Phase2,HHSize_Phase3))

WaterSaved_LeakAlarm <- round((LPCD_Phase1-LPCD_Phase2)*HHSize_Phase12*daydifference_1/1000)
WaterSaved_LeakAlarm_Percent <- round((LPCD_Phase1-LPCD_Phase2)/LPCD_Phase1*100,2)

WaterSaved_Gamification <- round((LPCD_Phase2-LPCD_Phase3)*HHSize_Phase23*daydifference_2/1000)
WaterSaved_Gamification_Percent <- round((LPCD_Phase2-LPCD_Phase3)/LPCD_Phase2*100,2)

Updated_DateTime_GlobalWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(LPCD_Phase1,LPCD_Phase2,LPCD_Phase3,
     WaterSaved_LeakAlarm,WaterSaved_LeakAlarm_Percent,
     WaterSaved_Gamification,WaterSaved_Gamification_Percent,
     Updated_DateTime_GlobalWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/GlobalWaterSavings.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_GlobalWaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)