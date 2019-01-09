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

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(Punggol_2016,Punggol_thisyear)
Punggol_All_SUB <- Punggol_All %>% dplyr::filter(meter_type=="SUB" & service_point_sn!="3100660792")

PunggolConsumption <- inner_join(Punggol_All_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_Block<- DailyConsumption %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date,block) %>%
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

## Water Savings Per Block Basis.
BlockLPCD_Phase1 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD)) 
BlockHHSize_Phase1 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(HHSize_Phase1=mean(TotalHH)) 
BlockLPCD_Phase2 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD)) 
BlockHHSize_Phase2 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(HHSize_Phase2=mean(TotalHH)) 
BlockHHSize_Phase12 <- cbind(BlockHHSize_Phase1,BlockHHSize_Phase2[,2]) %>%
                       dplyr::group_by(block) %>%
                       dplyr::mutate(HHSize_Phase12=mean(c(HHSize_Phase1,HHSize_Phase2)))
BlockLPCD_Phase3 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD)) 
BlockHHSize_Phase3 <- DailyLPCD_Block %>%
  dplyr::group_by(block) %>%
  dplyr::filter(date >="2017-06-10") %>% 
  dplyr::summarise(HHSize_Phase3=mean(TotalHH)) 
BlockHHSize_Phase23 <- cbind(BlockHHSize_Phase2,BlockHHSize_Phase3[,2]) %>%
                       dplyr::group_by(block) %>%
                       dplyr::mutate(HHSize_Phase23=mean(c(HHSize_Phase2,HHSize_Phase3)))

BlockWaterSaved_LeakAlarm <- cbind(cbind(BlockLPCD_Phase1,BlockLPCD_Phase2[2]),BlockHHSize_Phase12[4]) %>%
                             dplyr::mutate(Savings_w_LeakAlarm=round((LPCD_Phase1-LPCD_Phase2)*HHSize_Phase12*daydifference_1/1000),
                                           Savings_w_LeakAlarm_Percent=round((LPCD_Phase1-LPCD_Phase2)/LPCD_Phase1*100,2))

BlockWaterSaved_Gamification <- cbind(cbind(BlockLPCD_Phase2,BlockLPCD_Phase3[2]),BlockHHSize_Phase23[4]) %>%
                                dplyr::mutate(Savings_w_Gamification=round((LPCD_Phase2-LPCD_Phase3)*HHSize_Phase23*daydifference_2/1000),
                                              Savings_w_Gamification_Percent=round((LPCD_Phase2-LPCD_Phase3)/LPCD_Phase2*100,2))

BlockWaterSavingsValues <- cbind(BlockWaterSaved_LeakAlarm[,c(1,5:6)],BlockWaterSaved_Gamification[,c(1,5:6)])
BlockWaterSavingsValues[4] <- NULL
Updated_DateTime_BlockWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(BlockWaterSavingsValues,Updated_DateTime_BlockWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/BlockWaterSavings_V2.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockWaterSavings_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)