gc()
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

# Phase_1 – pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 – leak alarm only (from 2016-04-20" to 2017-06-10)
# Phase_3 – leak alarm + app (> 2017-06-10)

# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)    ---------------  (A)
# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  ---------------  (B)
# For (a) Global (b) Block (c) Individual basis

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
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_previous <- rbind(Punggol_2016,Punggol_2017)
Punggol_All <- rbind(Punggol_previous,Punggol_thisyear)
Punggol_All_SUB <- Punggol_All %>% dplyr::filter(meter_type=="SUB" & !service_point_sn %in% c("3100660792","3101127564"))
                                                                                          ## exlucde AHL and Child Care.

PunggolConsumption <- inner_join(Punggol_All_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type"))

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_Global <- DailyConsumption %>% dplyr::filter(date!=today()) %>%
                    dplyr::group_by(date) %>%
                    dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
                    dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD_Block<- DailyConsumption %>% dplyr::filter(date!=today()) %>%
                  dplyr::group_by(date,block) %>%
                  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
                  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD_Individual <- DailyConsumption %>% dplyr::filter(date!=today()) %>%
                        dplyr::group_by(date,service_point_sn) %>%
                        dplyr::mutate(DailyLPCD=DailyConsumption/num_house_member) %>% as.data.frame()

## -------- Water Savings ------------- ##
# Phase_1 : pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 : leak alarm only (from 2016-04-20" to 2017-06-09)
# Phase_3 : leak alarm + app (>= 2017-06-10)

# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)   
daydifference_1 <- as.numeric(difftime(today()-1,"2016-04-20",units=c("days")))

# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  
daydifference_2 <- as.numeric(difftime(today()-1,"2017-06-10",units=c("days")))

## (a) Gloabl Water Savings.
LPCD_Phase1 <- DailyLPCD_Global %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase1 <- DailyLPCD_Global %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(HHSize_Phase1=mean(TotalHH))  %>% as.numeric()
LPCD_Phase2 <- DailyLPCD_Global %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase2 <- DailyLPCD_Global %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(HHSize_Phase2=mean(TotalHH)) %>% as.numeric()
HHSize_Phase12 <- mean(c(HHSize_Phase1,HHSize_Phase2))
LPCD_Phase3 <- DailyLPCD_Global %>%
  dplyr::filter(date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD)) %>% as.numeric()
HHSize_Phase3 <- DailyLPCD_Global %>%
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

## (b) Block Water Savings. 
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
colnames_BlockWaterSavingsValues <- names(BlockWaterSavingsValues)

BlockWaterSavingsValues <- BlockWaterSavingsValues %>%  dplyr::group_by(block) %>%
  dplyr::mutate(LeakConsumption=100/Savings_w_LeakAlarm_Percent*Savings_w_LeakAlarm,
                GamificationConsumption=100/Savings_w_Gamification_Percent*Savings_w_Gamification) %>% as.data.frame()
BlockWaterSavingsValues_Total <- as.data.frame(list("Total",sum(BlockWaterSavingsValues$Savings_w_LeakAlarm),
                                                    round(sum(BlockWaterSavingsValues$Savings_w_LeakAlarm)/sum(BlockWaterSavingsValues$LeakConsumption)*100,2),
                                                    sum(BlockWaterSavingsValues$Savings_w_Gamification),
                                                    round(sum(BlockWaterSavingsValues$Savings_w_Gamification)/sum(BlockWaterSavingsValues$GamificationConsumption)*100,2)))

colnames(BlockWaterSavingsValues_Total) <- colnames_BlockWaterSavingsValues
BlockWaterSavingsValues <- rbind(BlockWaterSavingsValues[,1:5],BlockWaterSavingsValues_Total)

Updated_DateTime_BlockWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(BlockWaterSavingsValues,Updated_DateTime_BlockWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/BlockWaterSavings.RData")

## (c) Individual Water Savings.
DailyLPCD_Phase1 <- DailyLPCD_Individual %>%
  dplyr::group_by(service_point_sn,num_house_member) %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>%
  dplyr::summarise(AvgLPCD_Phase1=round(mean(DailyLPCD))) %>% as.data.frame()
DailyLPCD_Phase2 <- DailyLPCD_Individual %>%
  dplyr::group_by(service_point_sn,num_house_member) %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>%
  dplyr::summarise(AvgLPCD_Phase2=round(mean(DailyLPCD))) %>% as.data.frame()
DailyLPCD_Phase3 <- DailyLPCD_Individual %>%
  dplyr::group_by(service_point_sn,num_house_member) %>%
  dplyr::filter(date >="2017-06-10")  %>%
  dplyr::summarise(AvgLPCD_Phase3=round(mean(DailyLPCD))) %>% as.data.frame()

WaterSaved_Gamification <- cbind(DailyLPCD_Phase2,DailyLPCD_Phase3[3]) %>%
                           dplyr::mutate(LPCDSavings_w_Gamification=(AvgLPCD_Phase2-AvgLPCD_Phase3))

save(WaterSaved_Gamification,
     file="/srv/shiny-server/DataAnalyticsPortal/data/WaterSaved_Gamification.RData")

DailyLPCD_Block <- DailyLPCD_Block %>% dplyr::select_("date","block","DailyLPCD")
save(DailyLPCD_Block,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_Block.RData")
write.csv(DailyLPCD_Block,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_Block.csv",row.names = FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_WaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)