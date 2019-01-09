rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

## Include and Exclude Zero Consumption, 2016 and 2017 data only
## All SUB Consumption, including both Online and Offline
## Only Online SUB Consumption
## Only Offline SUB Consumption

# Phase_1 : pre leak alarm and app (from 2016-03-18 to 2016-04-19)
# Phase_2 : leak alarm only (from 2016-04-20" to 2017-06-10)
# Phase_3 : leak alarm + app (> 2017-06-10)
# LPCD Approach (preferred method)
# Actual Water savings due to Leak Alarm = LPCD (Phase_1) - LPCD (Phase_2)    ---------------  (A)
# Actual Water savings due to Gamification = LPCD (Phase_2) - LPCD (Phase_3)  ---------------  (B)

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
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE")
family_servicepoint_offline <- family_servicepoint %>% dplyr::filter(online_status=="INACTIVE")

Punggol_2016 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2016.fst")
Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[1:13]
Punggol_All <- rbind(Punggol_2016,Punggol_2017)
Punggol_All_SUB <- Punggol_All %>% dplyr::filter(meter_type=="SUB" & !service_point_sn %in% c("3100660792","3101127564"))
## exlucde AHL and Child Care.

###  -- this is just to calculate the Yearly Consumption for Punggol  --### 
YearlyPunggol <- Punggol_All_SUB %>% dplyr::filter(date(date_consumption)>="2016-10-01" &
                                                     date(date_consumption)<="2017-10-31" & meter_type=="SUB" &
                                                     service_point_sn %in% family_servicepoint$service_point_sn)
sum(YearlyPunggol$adjusted_consumption,na.rm = TRUE)

YearlyPunggol_Online <- YearlyPunggol %>% dplyr::filter(service_point_sn %in% family_servicepoint_online$service_point_sn)
YearlyPunggol_Offline <- YearlyPunggol %>% dplyr::filter(service_point_sn %in% family_servicepoint_offline$service_point_sn)

sum(YearlyPunggol_Online$adjusted_consumption,na.rm = TRUE)
sum(YearlyPunggol_Offline$adjusted_consumption,na.rm = TRUE)
###  -- this is just to calculate the Yearly Consumption for Punggol  --### 

############   All SUB Consumption, including both Online and Offline #########################
## find water consumed since 15 March 2016, excluding ChildCare
# TotalPunggolConsumption <- Punggol_All %>%
#   dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & service_point_sn %in% family_servicepoint$service_point_sn) %>%
#   dplyr::filter(date(adjusted_date)>="2016-03-15") %>%
#   dplyr::summarise(TotalPunggolConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>% as.numeric()

PunggolConsumption <- inner_join(Punggol_All_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption_IncludeZero <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>% as.data.frame()

DailyConsumption_ExcludeZero <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>% 
  dplyr::filter(DailyConsumption!=0) %>% as.data.frame()

DailyLPCD_IncludeZero <- DailyConsumption_IncludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyLPCD_ExcludeZero <- DailyConsumption_ExcludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption[DailyConsumption!=0]),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD=TotalDailyConsumption/TotalHH) %>% as.data.frame()

# Online
Punggol_All_SUB_Online <- Punggol_All_SUB %>% dplyr::filter(service_point_sn %in% family_servicepoint_online$service_point_sn)

PunggolConsumption_Online <- inner_join(Punggol_All_SUB_Online,family_servicepoint,by=c("service_point_sn","block","floor","room_type"))

DailyConsumption_Online_IncludeZero <- PunggolConsumption_Online %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_Online_IncludeZero<- DailyConsumption_Online_IncludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD_Online_IncludeZero=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyConsumption_Online_ExcludeZero <- PunggolConsumption_Online %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>%
  dplyr::filter(DailyConsumption!=0) %>% as.data.frame()

DailyLPCD_Online_ExcludeZero <- DailyConsumption_Online_ExcludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD_Online_ExcludeZero=TotalDailyConsumption/TotalHH) %>% as.data.frame()

# Offline
Punggol_All_SUB_Offline <- Punggol_All_SUB %>% dplyr::filter(service_point_sn %in% family_servicepoint_offline$service_point_sn)

PunggolConsumption_Offline <- inner_join(Punggol_All_SUB_Offline,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

DailyConsumption_Offline_IncludeZero <- PunggolConsumption_Offline %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD_Offline_IncludeZero <- DailyConsumption_Offline_IncludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD_Offline_IncludeZero=TotalDailyConsumption/TotalHH) %>% as.data.frame()

DailyConsumption_Offline_ExcludeZero <- PunggolConsumption_Offline %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) %>%
  dplyr::filter(DailyConsumption!=0) %>% as.data.frame()

DailyLPCD_Offline_ExcludeZero <- DailyConsumption_Offline_ExcludeZero %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date) %>%
  dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(DailyLPCD_Offline_ExcludeZero=TotalDailyConsumption/TotalHH) %>% as.data.frame()

Daily_LPCD_IncludeZero <- cbind(cbind(DailyLPCD_IncludeZero[,c(1,4)],DailyLPCD_Online_IncludeZero[,4]),
                                DailyLPCD_Offline_IncludeZero[,4])
colnames(Daily_LPCD_IncludeZero) <- c("Dates","DailyLPCD_IncludeZero","DailyLPCD_Online_IncludeZero",
                                      "DailyLPCD_Offline_IncludeZero")

Daily_LPCD_ExcludeZero <- cbind(cbind(DailyLPCD_ExcludeZero[,c(1,4)],DailyLPCD_Online_ExcludeZero[,4]),
                                DailyLPCD_Offline_ExcludeZero[,4])
colnames(Daily_LPCD_ExcludeZero) <- c("Dates","DailyLPCD_ExcludeZero","DailyLPCD_Online_ExcludeZero",
                                      "DailyLPCD_Offline_ExcludeZero")

## Gloabl LPCD Phase 1, Phase 2 and Phase 3, include and exclude Zero
LPCD_Phase1_IncludeZero <- DailyLPCD_IncludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD)) %>% as.numeric()
LPCD_Phase2_IncludeZero <- DailyLPCD_IncludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD)) %>% as.numeric()
LPCD_Phase3_IncludeZero <- DailyLPCD_IncludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD)) %>% as.numeric()

## Online
LPCD_Phase1_Online_IncludeZero <- DailyLPCD_Online_IncludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD_Online_IncludeZero)) %>% as.numeric()
LPCD_Phase2_Online_IncludeZero <- DailyLPCD_Online_IncludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD_Online_IncludeZero)) %>% as.numeric()
LPCD_Phase3_Online_IncludeZero <- DailyLPCD_Online_IncludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD_Online_IncludeZero)) %>% as.numeric()

## Offline
LPCD_Phase1_Offline_IncludeZero <- DailyLPCD_Offline_IncludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD_Offline_IncludeZero)) %>% as.numeric()
LPCD_Phase2_Offline_IncludeZero <- DailyLPCD_Offline_IncludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD_Offline_IncludeZero)) %>% as.numeric()
LPCD_Phase3_Offline_IncludeZero <- DailyLPCD_Offline_IncludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD_Offline_IncludeZero)) %>% as.numeric()

LPCD_Phase1_ExcludeZero <- DailyLPCD_ExcludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD)) %>% as.numeric()
LPCD_Phase2_ExcludeZero <- DailyLPCD_ExcludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD)) %>% as.numeric()
LPCD_Phase3_ExcludeZero <- DailyLPCD_ExcludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD)) %>% as.numeric()

## Online
LPCD_Phase1_Online_ExcludeZero <- DailyLPCD_Online_ExcludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD_Online_ExcludeZero)) %>% as.numeric()
LPCD_Phase2_Online_ExcludeZero <- DailyLPCD_Online_ExcludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD_Online_ExcludeZero)) %>% as.numeric()
LPCD_Phase3_Online_ExcludeZero <- DailyLPCD_Online_ExcludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD_Online_ExcludeZero)) %>% as.numeric()

## Offline
LPCD_Phase1_Offline_ExcludeZero <- DailyLPCD_Offline_ExcludeZero %>%
  dplyr::filter(date <="2016-04-19" & date >="2016-03-18") %>% 
  dplyr::summarise(LPCD_Phase1=mean(DailyLPCD_Offline_ExcludeZero)) %>% as.numeric()
LPCD_Phase2_Offline_ExcludeZero <- DailyLPCD_Offline_ExcludeZero %>%
  dplyr::filter(date <="2017-06-09" & date >="2016-04-20") %>% 
  dplyr::summarise(LPCD_Phase2=mean(DailyLPCD_Offline_ExcludeZero)) %>% as.numeric()
LPCD_Phase3_Offline_ExcludeZero <- DailyLPCD_Offline_ExcludeZero %>%
  dplyr::filter(date <="2017-12-31" & date >="2017-06-10") %>% 
  dplyr::summarise(LPCD_Phase3=mean(DailyLPCD_Offline_ExcludeZero)) %>% as.numeric()

save(Daily_LPCD_IncludeZero,Daily_LPCD_ExcludeZero,
     LPCD_Phase1_IncludeZero,LPCD_Phase2_IncludeZero,LPCD_Phase3_IncludeZero,
     LPCD_Phase1_ExcludeZero,LPCD_Phase2_ExcludeZero,LPCD_Phase3_ExcludeZero,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD_IncludeExcludeZero.RData")