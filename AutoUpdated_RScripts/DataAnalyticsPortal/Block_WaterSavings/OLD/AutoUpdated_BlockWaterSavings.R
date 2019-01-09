rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

# Phase_1 – pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 – leak alarm only (from 2016-04-20" to 2017-06-10)
# Phase_3 – leak alarm + app (> 2017-06-10)
# LPCD Approach (preferred method)
# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)    ---------------  (A)
# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  ---------------  (B)

# Leakage Rate Approach
# Potential water savings due to leak alarm =  { Leak rate (Phase_1) – Leak rate (Phase_2) } x no of days in Phase_2 + 
#                                              { Leak rate (Phase_2) – Leak rate (Phase_3) } x no of days in Phase_3

# We can use the total leak volume to “back-check” the robustness of the figures under the LPCD approach since 
# the actual volume of water saved under the LPCD approach should be the same as that computed under the leak rate method.

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(Punggol_2016,Punggol_thisyear)

today <- today()
family <- as.data.frame(tbl(con,"family") %>%
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE"
                                 & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

## find water consumed since 19 April 2016, excluding ChildCare
PunggolConsumption_PerBlock <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & service_point_sn %in% family_servicepoint$service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>="2016-04-19") %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(TotalPunggolConsumption=sum(adjusted_consumption,na.rm = TRUE))

## -------- Water Savings ------------- ##
# Phase_1 – pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 – leak alarm only (from 2016-04-20" to 2017-06-09)
# Phase_3 – leak alarm + app (>= 2017-06-10)
LPCD_Phase1 <- DailyLPCD_Block %>%
               dplyr::group_by(block) %>% 
               dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
               dplyr::summarise(LPCD_Phase1=mean(DailyLPCD))
HHSize_Phase1 <- DailyLPCD_Block %>%
                 dplyr::group_by(block) %>% 
                 dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
                 dplyr::summarise(HHSize_Phase1=mean(TotalHH))
LPCD_Phase2 <- DailyLPCD_Block %>%
               dplyr::group_by(block) %>% 
               dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
               dplyr::summarise(LPCD_Phase2=mean(DailyLPCD))
HHSize_Phase2 <- DailyLPCD_Block %>%
                 dplyr::group_by(block) %>% 
                 dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
                 dplyr::summarise(HHSize_Phase2=mean(TotalHH))
HHSize_Phase12 <- cbind(HHSize_Phase1,HHSize_Phase2[,2]) %>% 
                  dplyr::group_by(block) %>%
                  dplyr::mutate(HHSizePhase12=mean(c(HHSize_Phase1,HHSize_Phase2)))
LPCD_Phase3 <- DailyLPCD_Block %>%
               dplyr::group_by(block) %>% 
               dplyr::filter(date >="2017-06-10") %>% 
               dplyr::summarise(LPCD_Phase3=mean(DailyLPCD))
HHSize_Phase3 <- DailyLPCD_Block %>%
                 dplyr::group_by(block) %>% 
                 dplyr::filter(date >="2017-06-10") %>% 
                 dplyr::summarise(HHSize_Phase3=mean(TotalHH))
HHSize_Phase23 <- cbind(HHSize_Phase2,HHSize_Phase3[,2]) %>% 
  dplyr::group_by(block) %>%
  dplyr::mutate(HHSizePhase23=mean(c(HHSize_Phase2,HHSize_Phase3)))

# Actual Water savings due to Leak Alarm = LPCD (Phase_1) – LPCD (Phase_2)    ---------------  (A)
WaterSavings_LeakAlarm <- cbind(LPCD_Phase1,LPCD_Phase2[-1]) %>%
                          dplyr::mutate(DifferenceLPCD=LPCD_Phase1-LPCD_Phase2)
# Actual Water savings due to Gamification = LPCD (Phase_2) – LPCD (Phase_3)  ---------------  (B)
WaterSavings_Gamification <- cbind(LPCD_Phase2,LPCD_Phase3[-1]) %>%
                             dplyr::mutate(DifferenceLPCD=LPCD_Phase2-LPCD_Phase3)

Phase1_LPCD <- mean(WaterSavings_LeakAlarm$LPCD_Phase1)
Phase2_LPCD <- mean(WaterSavings_LeakAlarm$LPCD_Phase2)
Phase3_LPCD <- mean(WaterSavings_Gamification$LPCD_Phase3)

daydifference_1 <- as.numeric(difftime(today()-1,"2016-04-20",units=c("days")))
WaterSaved_LeakAlarm <- inner_join(WaterSavings_LeakAlarm,HHSize_Phase12,by="block") %>%
  dplyr::mutate(WaterSavedLeakAlarm=DifferenceLPCD*daydifference_1*HHSizePhase12) 

daydifference_2 <- as.numeric(difftime(today()-1,"2017-06-10",units=c("days")))
WaterSaved_Gamification <- inner_join(WaterSavings_Gamification,HHSize_Phase23,by="block") %>%
  dplyr::mutate(WaterSavedGamification=DifferenceLPCD*daydifference_2*HHSizePhase23) 

AverageDailyConsumption_After10June2017_Block <- DailyLPCD_Block %>% dplyr::filter(date>"2017-06-10" & date <= (today()-1)) %>% 
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageConsumption=mean(TotalDailyConsumption)) %>%
  dplyr::mutate(TotalPunggolConsumptionAfterGamification=AverageConsumption*daydifference_2) %>%
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
                                                    round(sum(BlockWaterSavingsValues$Savings_w_LeakAlarm)/sum(BlockWaterSavingsValues$OverallConsumption)*100,2),
                                                    round(sum(BlockWaterSavingsValues$Savings_w_Gamification)/sum(BlockWaterSavingsValues$ConsumptionAfterGamificationStarted)*100,2)))
colnames(BlockWaterSavingsValues_Total) <- names(BlockWaterSavingsValues)
BlockWaterSavingsValues <- rbind(BlockWaterSavingsValues,BlockWaterSavingsValues_Total)

Updated_DateTime_BlockWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(BlockWaterSavingsValues,Updated_DateTime_BlockWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/BlockWaterSavings.RData")

## multiple linear regression
## output: daily_LPCD, 
## inputs: daily mean temperature, weekends, holiday, price hike (1 July 2017 onwards), 
##         GamificationApp (10 June 2017 onwards), LeakAlarm (10 April 2016 onwards)

load("/srv/shiny-server/DataAnalyticsPortal/data/Weather.RData")
TempAvg <- Weather %>% dplyr::select_("Date","Tavg")
LPCD_Daily <- DailyLPCD %>% dplyr::select_("date","DailyLPCD")

data=read.csv("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_Study.csv") %>%
     dplyr::select_("LPCD","temp","weekend","holiday","price.hike","app","leak.alarm")

multi.fit = lm(LPCD~temp+weekend+holiday+price.hike+app+leak.alarm, data=data)
summary(multi.fit)
coefficients <- summary(multi.fit)$coefficients[6:7, 1]
coefficient_app <- coefficients[1]
coefficient_leakalarm <- coefficients[2]

WaterSavingsLPCD = data.table(ItemDescription=c("NoLeakAlarmNoGamification","LeakAlarmActivated",
                                                 "GamificationStarted"),
                              Duration=c("18-Mar-2016 to        19-Apr-2016","20-Apr-2016 to        10-Jun-2017","> 10-Jun-2017"),
                              LPCD=c(format(round(Phase1_LPCD,1),nsmall = 1),format(round(Phase2_LPCD,1),nsmall = 1),format(round(Phase3_LPCD,1),nsmall = 1)),
                              RegressionModel=c("--",abs(as.numeric(format(round(as.numeric(coefficient_leakalarm),1),nsmall = 1))),
                                                     abs(as.numeric(format(round(as.numeric(coefficient_app),1),nsmall = 1)))))

Updated_DateTime_WaterSavingsLPCD <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")
  
save(WaterSavingsLPCD,Updated_DateTime_WaterSavingsLPCD,
     file="/srv/shiny-server/DataAnalyticsPortal/data/WaterSavingsLPCD.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockWaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)