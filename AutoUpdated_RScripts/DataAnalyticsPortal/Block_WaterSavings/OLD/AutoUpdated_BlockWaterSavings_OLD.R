rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

today <- today()
flow <- as.data.frame(tbl(con,"flow"))
family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                 & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
meter <- as.data.frame(tbl(con,"meter"))

servicepoint_SUB <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL
servicepoint_SUB_meter <- inner_join(servicepoint_SUB,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

flow_servicepoint_SUB_meter <- inner_join(servicepoint_SUB_meter,flow,by=c("id.x"="id_service_point"))
flow_servicepoint_SUB_meter <- subset(flow_servicepoint_SUB_meter, select = c("service_point_sn","block","floor","meter_sn","site","min_5_flow","flow_date"))

## find water consumed since 19 April 2016, excluding ChildCare
PunggolConsumption_PerBlock <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & service_point_sn %in% family_servicepoint$service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>="2016-04-19") %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(TotalPunggolConsumption=sum(adjusted_consumption,na.rm = TRUE))

## -------- Water Savings ------------- ##
LeakVolume_Block1 <- flow_servicepoint_SUB_meter %>% 
  group_by(block) %>%
  filter(date(flow_date)>="2016-03-18" & date(flow_date)<="2016-04-19") %>%
  mutate(LeakVolumePerDay=min_5_flow*24) %>%
  dplyr::summarise(TotalLeakVolume1=sum(LeakVolumePerDay))

days_DateRange1 = as.integer(difftime("2016-04-19","2016-03-18",units = "days")) # days
LeakVolume_Block1 <- LeakVolume_Block1 %>% dplyr::mutate(AverageLeak1=TotalLeakVolume1/days_DateRange1)

LeakVolume_Block2 <- flow_servicepoint_SUB_meter %>% 
  group_by(block) %>%
  filter(date(flow_date)>="2016-04-20" & date(flow_date)<="2017-06-10") %>%
  mutate(LeakVolumePerDay=min_5_flow*24) %>%
  dplyr::summarise(TotalLeakVolume2=sum(LeakVolumePerDay))

days_DateRange2 = as.integer(difftime("2017-06-10","2016-04-20",units = "days")) # days
LeakVolume_Block2 <- LeakVolume_Block2 %>% dplyr::mutate(AverageLeak2=TotalLeakVolume2/days_DateRange2)

WaterSaved_LeakAlarm <- cbind(LeakVolume_Block1,LeakVolume_Block2[-1]) %>% 
                        dplyr::mutate(WaterSavedLeakAlarm=(AverageLeak1-AverageLeak2)*as.integer(difftime(today-1,"2016-04-20",units = "days"))) 

## find DailyLPCD (before 10 June 2017 & after 20 April 2016), and after 10 June 2017
DailyLPCD_Before_10June2017_After_20April2016 <- DailyLPCD_Block %>%
                                                 dplyr::group_by(block) %>% 
                                                 dplyr::filter(date <"2017-06-10" & date >"2016-04-20") %>% 
                                                 dplyr::summarise(AverageLPCD_Before=mean(DailyLPCD))
DailyLPCD_After_10June2017 <- DailyLPCD_Block %>% 
                              dplyr::group_by(block) %>% 
                              dplyr::filter(date >="2017-06-10" & date <= (today-1)) %>% 
                              dplyr::summarise(AverageLPCD_After=mean(DailyLPCD))

Difference_LPCD_Block <- cbind(DailyLPCD_Before_10June2017_After_20April2016,DailyLPCD_After_10June2017[-1]) %>%
                         dplyr::mutate(DifferenceLPCD=AverageLPCD_Before-AverageLPCD_After)

todayMonth = month.abb[month(today)-1]
daydifference_10June2017 <- as.numeric(difftime(today-1,"2017-06-10",units=c("days")))
WaterSaved_Gamification <- inner_join(Difference_LPCD_Block,familymembers_Block,by="block") %>%
                           dplyr::mutate(WaterSavedGamification=DifferenceLPCD*daydifference_10June2017*TotalBlockMember) 

AverageDailyConsumption_After10June2017_Block <- DailyLPCD_Block %>% dplyr::filter(date>"2017-06-10" & date <= (today-1)) %>% 
                                                 dplyr::group_by(block) %>%
                                                 dplyr::summarise(AverageConsumption=mean(TotalDailyConsumption)) %>%
                                                 dplyr::mutate(TotalPunggolConsumptionAfterGamification=AverageConsumption*daydifference_10June2017) %>%
                                                 dplyr::select_("block","TotalPunggolConsumptionAfterGamification")

BlockWaterSavings <- inner_join(WaterSaved_LeakAlarm,WaterSaved_Gamification,by="block") %>%
  dplyr::select_("block","WaterSavedLeakAlarm","WaterSavedGamification")

BlockWaterSavingsValues <- cbind(cbind(BlockWaterSavings,PunggolConsumption_PerBlock[-1]),AverageDailyConsumption_After10June2017_Block[-1]) %>%
  dplyr::mutate(Savings_w_LeakAlarm=round(WaterSavedLeakAlarm/1000),
                OverallConsumption=round(TotalPunggolConsumption/1000),
                Savings_w_Gamification=round(WaterSavedGamification/1000),
                ConsumptionAfterGamificationStarted=round(TotalPunggolConsumptionAfterGamification/1000))%>%
  dplyr::mutate(Savings_w_LeakAlarm_Percent=round(WaterSavedLeakAlarm/TotalPunggolConsumption*100,2),
                Savings_w_Gamification_Percent=round(WaterSavedGamification/TotalPunggolConsumptionAfterGamification*100,2)) %>%
  dplyr::select_("block","Savings_w_LeakAlarm","OverallConsumption","Savings_w_Gamification",
                 "ConsumptionAfterGamificationStarted","Savings_w_LeakAlarm_Percent","Savings_w_Gamification_Percent")
BlockWaterSavingsValues_Total <- as.data.frame(list("Total",sum(BlockWaterSavingsValues$Savings_w_LeakAlarm),sum(BlockWaterSavingsValues$OverallConsumption),
                                 sum(BlockWaterSavingsValues$Savings_w_Gamification),sum(BlockWaterSavingsValues$ConsumptionAfterGamificationStarted),
                                 sum(BlockWaterSavingsValues$Savings_w_LeakAlarm_Percent),sum(BlockWaterSavingsValues$Savings_w_Gamification_Percent)))
colnames(BlockWaterSavingsValues_Total) <- names(BlockWaterSavingsValues)
BlockWaterSavingsValues <- rbind(BlockWaterSavingsValues,BlockWaterSavingsValues_Total)

Updated_DateTime_BlockWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")
  
save(BlockWaterSavingsValues,Updated_DateTime_BlockWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/BlockWaterSavings.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockWaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)