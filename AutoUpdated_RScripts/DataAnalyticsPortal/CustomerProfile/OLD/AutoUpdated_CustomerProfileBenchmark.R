rm(list=ls(all=TRUE));invisible(gc());
cat('\014')

ptm <- proc.time()

load("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2/Output/01-Punggol_Indicators.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last30days <- today()-30
DailyCons_last30days <- DailyCons %>% dplyr::filter(Date>last30days) %>% 
                        dplyr::select_("ID","vol") %>%
                        dplyr::group_by(ID) %>%
                        dplyr::summarise(AvgDailyConsumption=round(mean(vol),2)) %>%
                        dplyr::rename(service_point_sn=ID)

Customers <- indicator %>% dplyr::select_("ID","block","num_house_member","adc") %>% 
             dplyr::rename(service_point_sn=ID)

DailyCons_last30days_Customers  <- inner_join(DailyCons_last30days,Customers,by="service_point_sn") %>%
                                   dplyr::mutate(AvgLPCD=round(AvgDailyConsumption/num_house_member,2)) %>%
                                   dplyr::select_("service_point_sn","AvgLPCD")

Customers_Details <- inner_join(Customers,family_details,by=c("service_point_sn","num_house_member")) %>%
  dplyr::select_("service_point_sn","block","num_house_member","online_status","adc") 

leak_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site=="Punggol")
load("/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")
PunggolZeroConsumption <- ZeroConsumptionCount %>% dplyr::filter(site=="Punggol")

## Offline
leak_open_Offline <- Customers_Details[match(leak_open$service_point_sn,Customers_Details$service_point_sn),] %>% dplyr::filter(online_status=="INACTIVE")
PunggolZeroConsumption_Offline <- Customers_Details[match(PunggolZeroConsumption$service_point_sn,Customers_Details$service_point_sn),] %>% 
  dplyr::filter(online_status=="INACTIVE")
HighCons_Offline <- Customers_Details %>% dplyr::filter(adc >=500 & online_status=="INACTIVE") %>% 
  dplyr::filter(!service_point_sn %in% leak_open_Offline$service_point_sn) %>%                                         
  dplyr::select_("service_point_sn","num_house_member")
LowCons_Offline <- Customers_Details %>% dplyr::filter(adc <500 & online_status=="INACTIVE") %>% 
  dplyr::filter(!service_point_sn %in% leak_open_Offline$service_point_sn) %>%  
  dplyr::filter(!service_point_sn %in% PunggolZeroConsumption_Offline$service_point_sn) %>%  
  dplyr::select_("service_point_sn","num_house_member")

## Online
leak_open_Online <- Customers_Details[match(leak_open$service_point_sn,Customers_Details$service_point_sn),] %>% dplyr::filter(online_status=="ACTIVE")
PunggolZeroConsumption_Online <- Customers_Details[match(PunggolZeroConsumption$service_point_sn,Customers_Details$service_point_sn),] %>% 
  dplyr::filter(online_status=="ACTIVE")
HighCons_Online <- Customers_Details %>% dplyr::filter(adc >=500 & online_status=="ACTIVE") %>% 
  dplyr::filter(!service_point_sn %in% leak_open_Online$service_point_sn) %>%  
  dplyr::select_("service_point_sn","num_house_member")
LowCons_Online <- Customers_Details %>% dplyr::filter(adc <500 & online_status=="ACTIVE") %>% 
  dplyr::filter(!service_point_sn %in% leak_open_Online$service_point_sn) %>%  
  dplyr::filter(!service_point_sn %in% PunggolZeroConsumption_Online$service_point_sn) %>%  
  dplyr::select_("service_point_sn","num_house_member")

## AvgLPCD, Offline
OpenLeakOffline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% leak_open_Offline$service_point_sn) %>%
                                             dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
OpenLeakOffline_AvgLPCD$AvgLPCD[is.nan(OpenLeakOffline_AvgLPCD$AvgLPCD)] <- NA

HighConsOffline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% HighCons_Offline$service_point_sn) %>%
                               dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
LowConsOffline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% LowCons_Offline$service_point_sn) %>%
                                            dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
ZeroConsumptionOffline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Offline$service_point_sn) %>%
                                                    dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
ZeroConsumptionOffline_AvgLPCD$AvgLPCD[is.nan(ZeroConsumptionOffline_AvgLPCD$AvgLPCD)] <- NA

## AvgLPCD, Online
OpenLeakOnline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% leak_open_Online$service_point_sn) %>%
                                            dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
OpenLeakOnline_AvgLPCD$AvgLPCD[is.nan(OpenLeakOnline_AvgLPCD$AvgLPCD)] <- NA

HighConsOnline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% HighCons_Online$service_point_sn) %>%
                                            dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
LowConsOnline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% LowCons_Online$service_point_sn) %>%
                                           dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
ZeroConsumptionOnline_AvgLPCD <- DailyCons_last30days_Customers %>% dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Online$service_point_sn) %>%
                                                   dplyr::summarize(AvgLPCD=round(mean(AvgLPCD)))
ZeroConsumptionOnline_AvgLPCD$AvgLPCD[is.nan(ZeroConsumptionOnline_AvgLPCD$AvgLPCD)] <- NA

## "average water savings LPCD with gamification" (to compare before/after gamification) (Before-After, using Phase 2 - Phase 3)
load("/srv/shiny-server/DataAnalyticsPortal/data/WaterSaved_Gamification.RData")
OpenLeakOffline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% leak_open_Offline$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
OpenLeakOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD[is.nan(OpenLeakOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD)] <- NA

HighConsOffline_water_savings_lpcd_Gamification <- WaterSaved_Gamification  %>% 
  dplyr::filter(service_point_sn %in% HighCons_Offline$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
LowConsOffline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% LowCons_Offline$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
ZeroConsumptionOffline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Offline$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
ZeroConsumptionOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD[is.nan(ZeroConsumptionOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD)] <- NA

OpenLeakOnline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% leak_open_Online$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
OpenLeakOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD[is.nan(OpenLeakOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD)] <- NA

HighConsOnline_water_savings_lpcd_Gamification <- WaterSaved_Gamification  %>% 
  dplyr::filter(service_point_sn %in% HighCons_Online$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
LowConsOnline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% LowCons_Online$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
ZeroConsumptionOnline_water_savings_lpcd_Gamification <- WaterSaved_Gamification %>% 
  dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Online$service_point_sn) %>%
  dplyr::summarize(AvgWaterSavingsLPCD=round(mean(LPCDSavings_w_Gamification,na.rm = TRUE)))
ZeroConsumptionOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD[is.nan(ZeroConsumptionOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD)] <- NA

## Avg Qty of HH members, Offline
leak_open_Offline_HHmembers <- leak_open_Offline %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
leak_open_Offline_HHmembers$HHmembers[is.nan(leak_open_Offline_HHmembers$HHmembers)] <- NA

HighCons_Offline_HHmembers <- HighCons_Offline %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
LowCons_Offline_HHmembers <- LowCons_Offline %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
PunggolZeroConsumption_Offline_HHmembers <- PunggolZeroConsumption_Offline %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))

## Avg Qty of HH members, Online
leak_open_Online_HHmembers <- leak_open_Online %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
if (nrow(leak_open_Online)==0){
  leak_open_Online_HHmembers$HHmembers=0
}
HighCons_Online_HHmembers <- HighCons_Online %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
LowCons_Online_HHmembers <- LowCons_Online %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
PunggolZeroConsumption_Online_HHmembers <- PunggolZeroConsumption_Online %>% dplyr::summarise(HHmembers=round(mean(num_house_member,na.rm = TRUE),1))
PunggolZeroConsumption_Online_HHmembers$HHmembers[is.nan(PunggolZeroConsumption_Online_HHmembers$HHmembers)] <- NA

## Avg Monthly Occupancy last month
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
lastmonth <- month(today())-1
thisYear <- year(today())
if (lastmonth==0){
  lastmonth=12
  thisYear=thisYear-1
}
monthly_occupancy_lastmonth <- monthly_occupancy %>% dplyr::filter(month(lastupdated)==lastmonth & year(lastupdated)==thisYear)

## Avg Monthly Occupancy last month, Offline
OpenLeakOffline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% leak_open_Offline$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
OpenLeakOffline_AvgOccupancy_lastmonth$AvgOccupancyRate[is.nan(OpenLeakOffline_AvgOccupancy_lastmonth$AvgOccupancyRate)] <- NA

HighConsOffline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% HighCons_Offline$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
LowConsOffline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% LowCons_Offline$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
PunggolZeroConsumptionOffline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Offline$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))

## Avg Daily Occupancy last week, Online
OpenLeakOnline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% leak_open_Online$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
if (is.na(OpenLeakOnline_AvgOccupancy_lastmonth)){
  OpenLeakOnline_AvgOccupancy_lastmonth$AvgOccupancyRate=0
}

HighConsOnline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% HighCons_Online$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
LowConsOnline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% LowCons_Online$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
PunggolZeroConsumptionOnline_AvgOccupancy_lastmonth  <- monthly_occupancy_lastmonth %>% 
  dplyr::filter(service_point_sn %in% PunggolZeroConsumption_Online$service_point_sn) %>%
  dplyr::summarise(AvgOccupancyRate=round(mean(occupancy_rate,na.rm=TRUE),1))
PunggolZeroConsumptionOnline_AvgOccupancy_lastmonth$AvgOccupancyRate[is.nan(PunggolZeroConsumptionOnline_AvgOccupancy_lastmonth$AvgOccupancyRate)] <- NA

Segmentation=c("OpenLeak","HighCons","LowCons","ZeroCons")
TotalCustomers <- nrow(Customers)
Benchmark <- data.frame(site=c(rep("Punggol",length(Segmentation)*2)),
                        online_status=c(rep("Offline",length(Segmentation)),rep("Online",length(Segmentation))),
                        Segmentation=c(Segmentation,Segmentation),
                        AvgLPCD=c(OpenLeakOffline_AvgLPCD$AvgLPCD,HighConsOffline_AvgLPCD$AvgLPCD,
                                      LowConsOffline_AvgLPCD$AvgLPCD,ZeroConsumptionOffline_AvgLPCD$AvgLPCD,
                                      OpenLeakOnline_AvgLPCD$AvgLPCD,HighConsOnline_AvgLPCD$AvgLPCD,
                                      LowConsOnline_AvgLPCD$AvgLPCD,ZeroConsumptionOnline_AvgLPCD$AvgLPCD),
                        AvgWaterSavingsLPCD_Gamification=c(OpenLeakOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           HighConsOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           LowConsOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD, 
                                                           ZeroConsumptionOffline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           OpenLeakOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           HighConsOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           LowConsOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD,
                                                           ZeroConsumptionOnline_water_savings_lpcd_Gamification$AvgWaterSavingsLPCD),
                        Qty_Households=c(nrow(leak_open_Offline),nrow(HighCons_Offline),nrow(LowCons_Offline),nrow(PunggolZeroConsumption_Offline),
                                         nrow(leak_open_Online),nrow(HighCons_Online),nrow(LowCons_Online),nrow(PunggolZeroConsumption_Online)),
                        Percent_SubTotalSite=c(round(nrow(leak_open_Offline)/TotalCustomers*100,1),
                                               round(nrow(HighCons_Offline)/TotalCustomers*100,1),
                                               round(nrow(LowCons_Offline)/TotalCustomers*100,1),
                                               round(nrow(PunggolZeroConsumption_Offline)/TotalCustomers*100,1),
                                               round(nrow(leak_open_Online)/TotalCustomers*100,1),
                                               round(nrow(HighCons_Online)/TotalCustomers*100,1),
                                               round(nrow(LowCons_Online)/TotalCustomers*100,1),
                                               round(nrow(PunggolZeroConsumption_Online)/TotalCustomers*100,1)),
                        AvgQtyHHmembers=c(leak_open_Offline_HHmembers$HHmembers,HighCons_Offline_HHmembers$HHmembers,
                                              LowCons_Offline_HHmembers$HHmembers,PunggolZeroConsumption_Offline_HHmembers$HHmembers,
                                              leak_open_Online_HHmembers$HHmembers,HighCons_Online_HHmembers$HHmembers,
                                              LowCons_Online_HHmembers$HHmembers,PunggolZeroConsumption_Online_HHmembers$HHmembers),
                        AvgOccupancy_lastmonth=c(OpenLeakOffline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         HighConsOffline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         LowConsOffline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         PunggolZeroConsumptionOffline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         OpenLeakOnline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         HighConsOnline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         LowConsOnline_AvgOccupancy_lastmonth$AvgOccupancyRate,
                                                         PunggolZeroConsumptionOnline_AvgOccupancy_lastmonth$AvgOccupancyRate))

Benchmark_Offline <- Benchmark %>% dplyr::filter(online_status=="Offline")
SubTotalOffline <- Benchmark_Offline %>%
                   dplyr::mutate(Qty_HouseholdsxAvgQtyHHmembers=Qty_Households*AvgQtyHHmembers,
                                 Qty_HouseholdsxAvgOccupancy_lastmonth=Qty_Households*AvgOccupancy_lastmonth) %>%
  dplyr::summarise(AvgLPCD=round(sum(AvgLPCD*Qty_Households*AvgQtyHHmembers)/sum(Qty_Households*AvgQtyHHmembers)),
                   AvgWaterSavingsLPCD_Gamification=round(sum(AvgWaterSavingsLPCD_Gamification*Qty_Households*AvgQtyHHmembers,na.rm=TRUE)/sum(Qty_Households*AvgQtyHHmembers)),
                   Qty_Households=sum(Qty_Households,na.rm=TRUE),
                   Percent_SubTotalSite=sum(Percent_SubTotalSite),
                   AvgQtyHHmembers=round(sum(Qty_HouseholdsxAvgQtyHHmembers)/Qty_Households,1),
                   AvgOccupancy_lastmonth=round(sum(Qty_HouseholdsxAvgOccupancy_lastmonth)/Qty_Households,1))

SubTotalOffline$site <- "Punggol"
SubTotalOffline$online_status <- "Offline"
SubTotalOffline$Segmentation <- "SubTotalOffline"
Benchmark_Offline <- rbind(Benchmark_Offline,SubTotalOffline)

Benchmark_Online <- Benchmark %>% dplyr::filter(online_status=="Online")
SubTotalOnline <- Benchmark_Online %>%
                  dplyr::mutate(Qty_HouseholdsxAvgQtyHHmembers=Qty_Households*AvgQtyHHmembers,
                                Qty_HouseholdsxAvgOccupancy_lastmonth=Qty_Households*AvgOccupancy_lastmonth) %>%
  dplyr::summarise(AvgLPCD=round(sum(AvgLPCD*Qty_Households*AvgQtyHHmembers,na.rm=TRUE)/sum(Qty_Households*AvgQtyHHmembers)),
                   AvgWaterSavingsLPCD_Gamification=round(sum(AvgWaterSavingsLPCD_Gamification*Qty_Households*AvgQtyHHmembers,na.rm=TRUE)/sum(Qty_Households*AvgQtyHHmembers)),
                   Qty_Households=sum(Qty_Households,na.rm=TRUE),
                   Percent_SubTotalSite=sum(Percent_SubTotalSite,na.rm=TRUE),
                   AvgQtyHHmembers=round(sum(Qty_HouseholdsxAvgQtyHHmembers)/Qty_Households,1),
                   AvgOccupancy_lastmonth=round(sum(Qty_HouseholdsxAvgOccupancy_lastmonth)/Qty_Households,1))
SubTotalOnline$site <- "Punggol"
SubTotalOnline$online_status <- "Online"
SubTotalOnline$Segmentation <- "SubTotalOnline"
Benchmark_Online <- rbind(Benchmark_Online,SubTotalOnline)

BenchmarkOfflineOnline <- rbind(Benchmark_Offline,Benchmark_Online)

PunggolTotal <- Benchmark %>%
                dplyr::mutate(Qty_HouseholdsxAvgQtyHHmembers=Qty_Households*AvgQtyHHmembers,
                              Qty_HouseholdsxAvgOccupancy_lastmonth=Qty_Households*AvgOccupancy_lastmonth) %>%
  dplyr::summarise(AvgLPCD=round(sum(AvgLPCD*Qty_Households*AvgQtyHHmembers,na.rm=TRUE)/sum(Qty_Households*AvgQtyHHmembers)),
                   AvgWaterSavingsLPCD_Gamification=round(sum(AvgWaterSavingsLPCD_Gamification*Qty_Households*AvgQtyHHmembers,na.rm=TRUE)/sum(Qty_Households*AvgQtyHHmembers)),
                   Qty_Households=sum(Qty_Households,na.rm=TRUE),
                   Percent_SubTotalSite=round(sum(Percent_SubTotalSite,na.rm=TRUE)),AvgQtyHHmembers=round(sum(Qty_HouseholdsxAvgQtyHHmembers)/Qty_Households,1),
                   AvgOccupancy_lastmonth=round(sum(Qty_HouseholdsxAvgOccupancy_lastmonth)/Qty_Households,1))

PunggolTotal$site <- "Punggol"
PunggolTotal$online_status <- "Offline/Online"
PunggolTotal$Segmentation <- "PunggolTotal"
Benchmark_Final <- rbind(BenchmarkOfflineOnline,PunggolTotal)

# replace NA with "NA" for DT
Benchmark_Final[6,4] <- "NA"
Benchmark_Final[6,5] <- "NA"

Updated_DateTime_CustomerProfileBenchmark <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Benchmark_Final,Updated_DateTime_CustomerProfileBenchmark,file="/srv/shiny-server/DataAnalyticsPortal/data/CustomerProfileBenchmark.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerProfileBenchmark successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)