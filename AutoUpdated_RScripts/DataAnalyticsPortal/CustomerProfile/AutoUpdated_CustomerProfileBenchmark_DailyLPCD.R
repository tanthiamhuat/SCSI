gc()
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

last30days <- today()-30

family <- as.data.frame(tbl(con,"family") %>%
             dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE"
             & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[,1:12]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol_All <- rbind(Punggol_2017,Punggol_thisyear)
Punggol_All_SUB <- Punggol_All %>% dplyr::filter(meter_type=="SUB" & service_point_sn!="3100660792")

PunggolConsumption <- inner_join(Punggol_All_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
                      dplyr::filter(date(adjusted_date)>= last30days)

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block,online_status) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

DailyLPCD<- DailyConsumption %>% dplyr::filter(date!=today()) %>%
  dplyr::group_by(date,service_point_sn) %>%
  dplyr::mutate(DailyLPCD=DailyConsumption/num_house_member) %>% as.data.frame()

leak_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site=="Punggol")
load("/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")
PunggolZeroConsumption <- ZeroConsumptionCount %>% dplyr::filter(site=="Punggol")

CustomerDetails <- DailyLPCD %>% dplyr::group_by(service_point_sn,online_status,num_house_member) %>%
                   dplyr::summarise(Avg_LPCD=round(mean(DailyLPCD))) %>%
                   dplyr::mutate(Segmentation=ifelse(service_point_sn %in% leak_open$service_point_sn,"OpenLeak",
                                              ifelse(service_point_sn %in% PunggolZeroConsumption$service_point_sn,"ZeroCons",
                                              ifelse(Avg_LPCD < 500, "LowCons",
                                              ifelse(Avg_LPCD >=500, "HighCons",0))))) 

## Avg Monthly Occupancy last month
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
lastmonth <- month(today())-1
thisYear <- year(today())
if (lastmonth==0){
  lastmonth=12
  thisYear=thisYear-1
}
monthly_occupancy_lastmonth <- monthly_occupancy %>% dplyr::filter(month(lastupdated)==lastmonth & year(lastupdated)==thisYear)

CustomerDetails_MonthlyOccupancy <- inner_join(CustomerDetails,monthly_occupancy_lastmonth,by="service_point_sn") %>%
                                    dplyr::select_("service_point_sn","online_status","num_house_member","Avg_LPCD","Segmentation","occupancy_rate")

## "average water savings LPCD with gamification" (to compare before/after gamification) (Before-After, using Phase 2 - Phase 3)
load("/srv/shiny-server/DataAnalyticsPortal/data/WaterSaved_Gamification.RData")

CustomerDetails_MonthlyOccupancy_WaterSavedGamification <- inner_join(CustomerDetails_MonthlyOccupancy,WaterSaved_Gamification,by=c("service_point_sn","num_house_member")) %>%
                                                           dplyr::select_("service_point_sn","online_status","num_house_member","Avg_LPCD","Segmentation","occupancy_rate","LPCDSavings_w_Gamification")
                                                                      
Benchmark <- CustomerDetails_MonthlyOccupancy_WaterSavedGamification %>%
             dplyr::group_by(online_status,Segmentation) %>%
             dplyr::summarise(AvgLPCD=round(mean(Avg_LPCD)),AvgLPCDSavingsGamification=round(mean(LPCDSavings_w_Gamification),1),
                              AvgQtyHHmember=round(mean(num_house_member),1),QtyHH=n_distinct(service_point_sn),
                              AvgMonthlyOccupancy=round(mean(occupancy_rate),1))

Benchmark_Online <- Benchmark %>% dplyr::filter(online_status=="ACTIVE") %>% as.data.frame()
Benchmark_Online_MissingSegmentation <- unique(Benchmark$Segmentation)[which(!unique(Benchmark$Segmentation) %in% Benchmark_Online$Segmentation)]
if (NROW(Benchmark_Online_MissingSegmentation)!=0) {
  Benchmark_Online_MissingSegmentation_data <- data.frame(online_status="ACTIVE",Segmentation=Benchmark_Online_MissingSegmentation,
                                                          AvgLPCD=0,AvgLPCDSavingsGamification=0,AvgQtyHHmember=0,QtyHH=0,AvgMonthlyOccupancy=0)
  Benchmark_Online_All <- rbind(Benchmark_Online,Benchmark_Online_MissingSegmentation_data)
 } else {
 Benchmark_Online_All <- Benchmark_Online
}
Benchmark_Online_SubTotal <- data.frame(online_status="ACTIVE",Segmentation="SubTotalOnline",
                                        AvgLPCD=round(sum(Benchmark_Online_All$AvgLPCD*Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH)/
                                                sum(Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH)),
                                        AvgLPCDSavingsGamification=round(sum(Benchmark_Online_All$AvgLPCDSavingsGamification*Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH)/
                                                sum(Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH),1),
                                        AvgQtyHHmember=round(sum(Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH)/sum(Benchmark_Online_All$QtyHH),1),
                                        QtyHH=sum(Benchmark_Online_All$QtyHH),
                                        AvgMonthlyOccupancy=round(sum(Benchmark_Online_All$AvgMonthlyOccupancy*Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH)/
                                                                  sum(Benchmark_Online_All$AvgQtyHHmember*Benchmark_Online_All$QtyHH),1))

Benchmark_Online <- rbind(Benchmark_Online_All,Benchmark_Online_SubTotal)

Benchmark_Offline <- Benchmark %>% dplyr::filter(online_status=="INACTIVE") %>% as.data.frame()
Benchmark_Offline_MissingSegmentation <- unique(Benchmark$Segmentation)[which(!unique(Benchmark$Segmentation) %in% Benchmark_Offline$Segmentation)]
if (NROW(Benchmark_Offline_MissingSegmentation)!=0) {
  Benchmark_Offline_MissingSegmentation_data <- data.frame(online_status="INACTIVE",Segmentation=Benchmark_Offline_MissingSegmentation,
                                                          AvgLPCD=0,AvgLPCDSavingsGamification=0,AvgQtyHHmember=0,QtyHH=0,AvgMonthlyOccupancy=0)
  Benchmark_Offline_All <- rbind(Benchmark_Offline,Benchmark_Offline_MissingSegmentation_data)
} else {
  Benchmark_Offline_All <- Benchmark_Offline
}
Benchmark_Offline_SubTotal <- data.frame(online_status="INACTIVE",Segmentation="SubTotalOffline",
                                        AvgLPCD=round(sum(Benchmark_Offline$AvgLPCD*Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH)/
                                                        sum(Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH)),
                                        AvgLPCDSavingsGamification=round(sum(Benchmark_Offline$AvgLPCDSavingsGamification*Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH)/
                                                                           sum(Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH),1),
                                        AvgQtyHHmember=round(sum(Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH)/sum(Benchmark_Offline$QtyHH),1),
                                        QtyHH=sum(Benchmark_Offline$QtyHH),
                                        AvgMonthlyOccupancy=round(sum(Benchmark_Offline$AvgMonthlyOccupancy*Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH)/
                                                                    sum(Benchmark_Offline$AvgQtyHHmember*Benchmark_Offline$QtyHH),1))

Benchmark_Offline <- rbind(Benchmark_Offline_All,Benchmark_Offline_SubTotal)
Benchmark_OnlineOffline <- rbind(Benchmark_Online,Benchmark_Offline)

Benchmark_OnlineOffline_MinusSubTotal <- Benchmark_OnlineOffline %>% dplyr::filter(Segmentation %in% Benchmark$Segmentation)
Benchmark_Total <- data.frame(online_status="ACTIVE/INACTIVE",Segmentation="PunggolTotal",
                              AvgLPCD=round(sum(Benchmark_OnlineOffline_MinusSubTotal$AvgLPCD*Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH)/
                                            sum(Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH)),
                              AvgLPCDSavingsGamification=round(sum(Benchmark_OnlineOffline_MinusSubTotal$AvgLPCDSavingsGamification*Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH)/
                                                               sum(Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH),1),
                              AvgQtyHHmember=round(sum(Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH)/sum(Benchmark_OnlineOffline_MinusSubTotal$QtyHH),1),
                              QtyHH=sum(Benchmark_OnlineOffline_MinusSubTotal$QtyHH),
                              AvgMonthlyOccupancy=round(sum(Benchmark_OnlineOffline_MinusSubTotal$AvgMonthlyOccupancy*Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH)/
                                                        sum(Benchmark_OnlineOffline_MinusSubTotal$AvgQtyHHmember*Benchmark_OnlineOffline_MinusSubTotal$QtyHH),1))

Benchmark_Final <- rbind(Benchmark_OnlineOffline,Benchmark_Total)

Updated_DateTime_CustomerProfileBenchmark <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Benchmark_Final,Updated_DateTime_CustomerProfileBenchmark,file="/srv/shiny-server/DataAnalyticsPortal/data/CustomerProfileBenchmark.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerProfileBenchmark_DailyLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)