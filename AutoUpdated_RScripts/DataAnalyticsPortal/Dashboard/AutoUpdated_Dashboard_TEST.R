rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst,geosphere)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last30days.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/BillableMeters_IndexChecks_last30days.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD_OnOffline.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/UnexpectedConsumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")

flow <- as.data.frame(tbl(con,"flow"))
family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                                        & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE")
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_MAINBYPASS <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type")) %>%
                           filter(site=="Punggol" & meter_type %in% c("MAIN","BYPASS"))

servicepoint_SUB <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL
servicepoint_SUB_meter <- inner_join(servicepoint_SUB,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

last10weeks <- Week.date[(nrow(Week.date)-10):(nrow(Week.date)-1),1]
today <- today()

## -------- Total Consumption ------------- ##
# Total consumption of submeters excluding industrials customers over the last 10 weeks 
# (mini chart over 10 weeks with horizontal unit = 1 week)
weekly_consumption_last10weeks <- data.frame(tbl(con,"weekly_consumption")) %>% 
                                  dplyr::filter(week_number %in% last10weeks) %>%
                                  group_by(week_number) %>%
                                  dplyr::summarise(WeeklyConsumption=sum(actual_consumption,na.rm = TRUE)/1000) 

weekly_consumption_last10weeks$week_number <- factor(weekly_consumption_last10weeks$week_number,
                                                     levels=weekly_consumption_last10weeks$week_number)

#weekly_consumption_last10weeks$week_number <- substr(weekly_consumption_last10weeks$week_number,6,7)
weekly_consumption_last10weeks$WeeklyConsumption <- round(weekly_consumption_last10weeks$WeeklyConsumption)

# Breakdown Consumption per block % (Donut chart)
# ConsumptionBreakdown <- Punggol_All %>% 
#                         dplyr::filter(meter_type=="SUB") %>%
#                         group_by(block) %>%
#                         dplyr::summarise(TotalConsumption=sum(adjusted_consumption,na.rm = TRUE)) 
# ConsumptionBreakdown$Percent <- round(ConsumptionBreakdown$TotalConsumption/sum(ConsumptionBreakdown$TotalConsumption)*100)

## -------- Leaks and Anomalies ------------- ##
# Leak Volume per day (mini chart of the last 10 days) and 
LeakVolumePerDay_last10days <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay_OnlineOfflineTotal.csv") %>%
  dplyr::arrange(desc(Date)) %>%
  dplyr::select_("Date","LeakVolumePerDay_Total") %>%
  head(10)
LeakVolumePerDay_last10days <- LeakVolumePerDay_last10days %>% 
  dplyr::mutate(Date1=paste(month.abb[month(Date)],"-",substr(Date,9,10),sep=""),
                Date2=paste(substr(Date,6,7),substr(Date,9,10),sep="/"))

# Total Quantity of opened leaks (raw number) and 
# trend (based on average quantity of opened leaks over the last 7 days versus last 30 days)
# +/- 5% -> constant
last7days <- today()-7
last30days <- today()-30
last90days <- today()-90
leak_alarm_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site=="Punggol")
leak_alarm_open_last7days <- leak_alarm_open %>% filter (start_date >=last7days)
leak_alarm_open_last30days <- leak_alarm_open %>% filter (start_date >=last30days)
if (nrow(leak_alarm_open_last7days) < nrow(leak_alarm_open_last30days)){
  leak_alarm_open_trend <- "Decreasing"
} else if (nrow(leak_alarm_open_last7days) > nrow(leak_alarm_open_last30days)) {
  leak_alarm_open_trend <- "Increasing"
} else if (nrow(leak_alarm_open_last7days) >= -0.05*nrow(leak_alarm_open_last30days) &
           nrow(leak_alarm_open_last7days) <= 0.05*nrow(leak_alarm_open_last30days)) {
  leak_alarm_open_trend <- "Constant"
} else if (nrow(leak_alarm_open_last7days) == nrow(leak_alarm_open_last30days)) {
  leak_alarm_open_trend <- "Constant"
}


# trend (based on average leak volume over the last 7 days versus last 30 days.)
LeakVolumePerDay_lastest <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay.csv") %>%
  dplyr::arrange(desc(Date)) %>%
  head(1) %>% dplyr::select_("TotalLeakVolumePerDay")

LeakVolumePerDay_last7days <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay.csv") %>%
  dplyr::arrange(desc(Date)) %>%
  head(7) %>%
  dplyr::summarise(average_leakvolume=mean(TotalLeakVolumePerDay))

LeakVolumePerDay_last30days <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay.csv") %>%
  dplyr::arrange(desc(Date)) %>%
  head(30) %>%
  dplyr::summarise(average_leakvolume=mean(TotalLeakVolumePerDay))

if (LeakVolumePerDay_last7days < LeakVolumePerDay_last30days){
  LeakVolumePerDay_trend <- "Decreasing"
} else if (LeakVolumePerDay_last7days > LeakVolumePerDay_last30days) {
  LeakVolumePerDay_trend <- "Increasing"
} else if (LeakVolumePerDay_last7days >= -0.05*LeakVolumePerDay_last30days &
           LeakVolumePerDay_last7days <= 0.05*LeakVolumePerDay_last30days) {
  LeakVolumePerDay_trend <- "Constant"
}

# Average duration of the leaks and 
# trend (based on average quantity of opened leaks over the last 7 days versus last 30 days)
leak_alarm_duration <- as.data.frame(tbl(con,"leak_alarm")) %>%
  dplyr::filter(site=="Punggol") %>%
  dplyr::summarise(average_duration = mean(duration))
leak_alarm_duration_last7days <- as.data.frame(tbl(con,"leak_alarm")) %>%
  dplyr::filter (start_date >=last7days & site=="Punggol") %>%
  dplyr::summarise(average_duration = mean(duration))
leak_alarm_duration_last90days <- as.data.frame(tbl(con,"leak_alarm")) %>%
  dplyr::filter (start_date >=last90days & site=="Punggol") %>%
  dplyr::summarise(average_duration = mean(duration))
if (is.na(leak_alarm_duration_last7days$average_duration) |
    leak_alarm_duration_last7days$average_duration < leak_alarm_duration_last90days$average_duration){
  leak_alarm_duration_trend <- "Decreasing"
} else if (leak_alarm_duration_last7days$average_duration > leak_alarm_duration_last90days$average_duration) {
  leak_alarm_duration_trend <- "Increasing"
} else if (leak_alarm_duration_last7days$average_duration >= -0.05*leak_alarm_duration_last90days$average_duration &
           leak_alarm_duration_last7days$average_duration <= 0.05*leak_alarm_duration_last90days$average_duration)  {
  leak_alarm_duration_trend <- "Constant"
}

LeakInformation_DT = data.frame(ItemDescription=c("Open Leaks","Leak Volume (litres)","Average Leak Duration (days)"),
                                Values=c(nrow(leak_alarm_open),LeakVolumePerDay_lastest$TotalLeakVolumePerDay,
                                         round(leak_alarm_duration$average_duration)),
                                Trend=c(leak_alarm_open_trend,LeakVolumePerDay_trend,leak_alarm_duration_trend))

# Quantity of Overconsumption alerts per week (mini chart over last 10 weeks with horizontal unit= 1 week)
overconsumption_alarm_online <- as.data.frame(tbl(con,"overconsumption_alarm")) %>% dplyr::filter(service_point_sn %in% family_servicepoint_online$service_point_sn)

overconsumption_alarm_Week <- inner_join(overconsumption_alarm_online,Week.date,by=c("overconsumption_date"="end")) %>%
  group_by(week) %>%
  dplyr::summarise(Count=n()) %>%
  dplyr::arrange(desc(week))

overconsumption_alarm_past10weeks <- head(overconsumption_alarm_Week, 10)

# Net consumption rate => color code per block:
# Green: 
#   no leak on any of the block meters (main or bypass) based on min 5 flow = based on open leak in leak alarm table 
# AND 
#   [net weekly consumption rate < 3% (TBD) for blocks without common fittings or < 5% (TBD) with common fittings] 
# 
# Orange:
#   no leak on any of the block meters (main or bypass) based on min 5 flow = based on open leak in leak alarm table
# AND
#   [net weekly consumption rate is between 3 and 5% (TBD) for blocks without common fittings or between 5 and 10% (TBD) for blocks with common fittings]
# 
# Red: 
#   1 leak on any of the block meters (main or bypass) based on min 5 flow = based on open leak in leak alarm table) 
# OR 
#   [net weekly consumption rate > 5% (TBD) for blocks without common fittings or > 10% (TBD) for blocks with common fittings]

# PG_B1 (with common fittings, CF)
# PG_B2 (with common fittings, CF)
# PG_B3 (with common fittings (upper), NCF (lower))
# PB_B4 (with common fittings (upper), NCF (lower))
# PG_B5 (with common fittings (upper), NCF (lower))

MAINBYPASS_CF <- servicepoint_MAINBYPASS %>% # only Punggol MAIN and BYPASS meters
                 dplyr::select_("id.x","service_point_sn","meter_sn","block","floor","site","status") %>%
                 dplyr::arrange(block) %>%
                 dplyr::mutate(CommonFittings=c(rep("CF",6),rep(c("CF","CF","NCF"),2),"NCF","CF","CF")) %>% # refer to CommonFittings.png in /www directory
                 dplyr::mutate(Indirect=ifelse(grepl("M|B",service_point_sn),1,0)) 
MAINBYPASS_CF_Reorder <- MAINBYPASS_CF[c(1:12,14:15,13),]

Blocks_CommonFittings <- MAINBYPASS_CF_Reorder %>% dplyr::select_("block","CommonFittings","Indirect") %>% unique()

#load('/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.RData')
load('/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData')

WeeklyNetConsumptionRateDirectIndirect <- WeeklyNetConsumption_NA %>% dplyr::select_("LastDayofWeek","block","DirectNetConsumptionRate","IndirectNetConsumptionRate") %>% head(5)

leak_alarm_Open_MAINBYPASS <- as.data.frame(tbl(con,"leak_alarm")) %>% dplyr::filter(status=="Open" & site=="Punggol" & meter_type %in% c("MAIN","BYPASS"))

MAINBYPASS_CF_Leak <- MAINBYPASS_CF %>%
                      dplyr::mutate(Leak=ifelse(service_point_sn %in% leak_alarm_Open_MAINBYPASS$service_point_sn,1,0)) %>%
                      dplyr::group_by(block,Indirect) %>%
                      dplyr::summarise(TotalLeak=sum(Leak))

WeeklyNetConsumptionRateDirectIndirect_Leak <- inner_join(WeeklyNetConsumptionRateDirectIndirect,MAINBYPASS_CF_Leak,by="block") 

# keycol <- "NetConsumption"
# valuecol <- "NetConsumptionRate"
# gathercols <- c("IndirectNetConsumptionRate", "DirectNetConsumptionRate")
# 
# WeeklyNetConsumptionRateDirectIndirect_Leak_Long <- gather_(WeeklyNetConsumptionRateDirectIndirect_Leak, keycol, valuecol, gathercols) %>%
#                                                     dplyr::arrange(block) %>% dplyr::select_("NetConsumptionRate","TotalLeak")
# Blocks_CommonFittings_NetConsumptionRate <- cbind(Blocks_CommonFittings,WeeklyNetConsumptionRateDirectIndirect_Leak_Long)

Blocks_CommonFittings_NetConsumptionRate <- inner_join(Blocks_CommonFittings,WeeklyNetConsumptionRateDirectIndirect_Leak,by=c("block","Indirect"))

Blocks_CommonFittings_NetConsumptionRate_Status <- Blocks_CommonFittings_NetConsumptionRate %>% 
                                                   dplyr::mutate(Status=ifelse(TotalLeak==0 & Indirect==0 & NetConsumptionRate < 4,"Green",
                                                                        ifelse(TotalLeak==0 & Indirect==0 & NetConsumptionRate >=4 & NetConsumptionRate <=7,"Orange",
                                                                        ifelse((TotalLeak==1 & Indirect==0 ) | NetConsumptionRate > 7 ,"Red",
                                                                        ifelse(TotalLeak==0 & Indirect==1 & NetConsumptionRate < 5,"Green",
                                                                        ifelse(TotalLeak==0 & Indirect==1 & NetConsumptionRate >=5 & NetConsumptionRate <=10,"Orange",
                                                                        ifelse((TotalLeak==1 & Indirect==1) | NetConsumptionRate > 10 ,"Red",0)))))))

Blocks_CommonFittings_NetConsumptionRate_Status <- Blocks_CommonFittings_NetConsumptionRate_Status %>%
                                                   dplyr::mutate(Status=ifelse(NetConsumptionRate=="NA","Grey",Status))

PunggolNetConsumptionOutput <- Blocks_CommonFittings_NetConsumptionRate_Status %>%
                        dplyr::select_("block","Indirect","Status")
PunggolNetConsumptionOutput_DirectIndirect <- spread(PunggolNetConsumptionOutput, Indirect, Status)
colnames(PunggolNetConsumptionOutput_DirectIndirect) <- c("Block","Direct","Indirect")

## Yuhua NetConsumptionOutput
## Dummy
YuhuaNetConsumptionOutput_DirectIndirect <- data.frame(Block=c("YH_B1","YH_B2","YH_B3","YH_B4","YH_B5","YH_B6","YH_B7"),
                                                       Direct=rep("Grey",7),Indirect=rep("Grey",7))

## -------- Water Savings ------------- ##
# WaterSaved_LeakAlarm <- BlockWaterSavingsValues$Savings_w_LeakAlarm[nrow(BlockWaterSavingsValues)]
# WaterSavedLeakAlarm_Percent <- BlockWaterSavingsValues$Savings_w_LeakAlarm_Percent[nrow(BlockWaterSavingsValues)]
# WaterSaved_Gamification <- BlockWaterSavingsValues$Savings_w_Gamification[nrow(BlockWaterSavingsValues)]
# WaterSavedGamification_Percent <- BlockWaterSavingsValues$Savings_w_Gamification_Percent[nrow(BlockWaterSavingsValues)]

## -------- Customer Engagement ------------- ##
OnlineCustomers <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE" & site=="Punggol")
OfflineCustomers <- family_servicepoint %>% dplyr::filter(online_status=="INACTIVE" & site=="Punggol")

WeeklyLPCD_OnOffline <- WeeklyLPCD_OnOffline %>% tail(10)
WeeklyLPCD_OnOffline["Year"] <- NULL

## -------- Overall Quantities ------------- ##
# PUNGGOL	YUHUA	TUAS 
# Qty of main meters and bypass	
# Qty of submeters 
# Qty of households: (active customers with Roomtype=1,2,3,4or5)
# Qty of occupied households 
# Qty of occupied with no consumption for the last 30 days 
# Qty of vacant households 
# Qty of vacant households with consumption

familyALL <- as.data.frame(tbl(con,"family"))
family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
  group_by(id_service_point) %>%
  dplyr::filter(move_in_date==max(move_in_date))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% as.data.frame()
familyALL_servicepoint <- inner_join(familyALL,servicepoint,by=c("id_service_point"="id","room_type")) %>% as.data.frame()

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

Punggol_Main <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & meter_type=="MAIN" & status=="ACTIVE")
Punggol_ByPass <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & meter_type=="BYPASS" & status=="ACTIVE")
Punggol_Sub <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & meter_type=="SUB" & status=="ACTIVE")
Yuhua_Main <- servicepoint_meter %>% dplyr::filter(site=="Yuhua" & meter_type=="MAIN" & status=="ACTIVE")
Yuhua_ByPass <- servicepoint_meter %>% dplyr::filter(site=="Yuhua" & meter_type=="BYPASS" & status=="ACTIVE")
Yuhua_Sub <- servicepoint_meter %>% dplyr::filter(site=="Yuhua" & meter_type=="SUB" & status=="ACTIVE")
Tuas_Main <- servicepoint_meter %>% dplyr::filter(site=="Tuas" & meter_type=="MAIN" & status=="ACTIVE")
Tuas_ByPass <- servicepoint_meter %>% dplyr::filter(site=="Tuas" & meter_type=="BYPASS" & status=="ACTIVE")

Punggol_Active <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & status=="ACTIVE" & !room_type %in% c("NIL","HDBCD")) %>% collect()
Yuhua_Active <- servicepoint_meter %>% dplyr::filter(site=="Yuhua" & status=="ACTIVE" & !room_type %in% c("NIL","HDBCD")) %>% collect()

Punggol_Occupied <- family_servicepoint %>% dplyr::filter(site=="Punggol" & status=="ACTIVE") %>% collect()
Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

consumption_last30days <-read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption_last30days.fst")

consumption_last30days_servicepoint <- inner_join(consumption_last30days,servicepoint,by=c("id_service_point"="id")) 

# PunggolConsumption_SUB_last30days <- consumption_last30days_servicepoint %>%
#   dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & 
#                   service_point_sn %in% familyALL_servicepoint$service_point_sn & site=="Punggol") %>%
#   select(service_point_sn,adjusted_consumption,date_consumption) %>%
#   dplyr::filter(date(date_consumption)>=last30days) %>%
#   group_by(service_point_sn) %>%
#   dplyr::summarise(TotalConsumption=sum(adjusted_consumption,na.rm=TRUE))

# Occupied_HH_zero_consumption <- dplyr::setdiff(PunggolConsumption_SUB_last30days[which(PunggolConsumption_SUB_last30days$TotalConsumption==0),1],
#                                                Punggol_Real_Vacant_ServicePoint_Sn)

Occupied_HH_zero_consumption <- ZeroConsumptionCount %>% dplyr::filter(site=="Punggol")

OverallQuantities = data.table(ItemDescription=c("Qty (Master+ByPass) Meters","Qty SUB Meters",
                                                  "Qty of households (active customers)",
                                                  "Qty Occupied Household","Qty Occupied HH (zero consumption)",
                                                  "Qty Vacant Household","Qty Vacant HH (non-zero consumption)"),
                                Punggol=c(as.data.table(tally(Punggol_Main))+as.data.table(tally(Punggol_ByPass)),as.data.table(tally(Punggol_Sub)),
                                          nrow(Punggol_Active),
                                          nrow(Punggol_Occupied),nrow(Occupied_HH_zero_consumption),
                                          length(Punggol_Real_Vacant),nrow(unexpected_consumption)),
                                Tuas=c(nrow(Tuas_Main)+nrow(Tuas_ByPass),rep(NA,6)),
                                Yuhua=c(as.data.table(tally(Yuhua_Main))+as.data.table(tally(Yuhua_ByPass)),as.data.table(tally(Yuhua_Sub)),
                                        nrow(Yuhua_Active),rep(NA,4)))

## -------- Data Quality ------------- ##
## in AWS it is showings the Map, while in GDC it is showing the Plotly plot
AverageHourlyReadingRate_Punggol <- HourlyIndexReadingRate_Punggol %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

AverageHourlyReadingRate_Yuhua <- NA

QuantityNoDataMeter <- BillableMeters[nrow(BillableMeters),ncol(BillableMeters)]

AverageHourlyReadingRate <- HourlyIndexReadingRate_PunggolBlocks %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

blocks <- sort(unique(HourlyIndexReadingRate_PunggolBlocks$block))
ReadingRate_blocks <- list()
for (i in 1:length(blocks)){
  ReadingRate_blocks[[i]] <- AverageHourlyReadingRate %>% filter(block==blocks[i]) %>% dplyr::select_("AverageHourlyReadingRate")
}
ReadingRate_blocks <- c(unlist(ReadingRate_blocks))

## http://www.latlong.net/convert-address-to-lat-long.html
## PG_B1, 103C Edgefield Plains, Lat=1.397414,Lon=103.904557
## PG_B2, 199C Punggol Field, Lat=1.400575,Lon=103.905878
## PG_B3, 266A Punggol Way,Lat=1.405382,Lon=103.897793
## PG_B4, 613C, Punggol Drive, Lat=1.404082,Lon=103.908573
## PG_B5, 624C Punggol Central, Lat=1.399853,Lon=103.911212
## Yuhua, Latitude=1.3245370, Longitude=103.7425669
PG_103C_LongLat <- c(103.904557,1.397414)
PG_199C_LongLat <- c(103.905878,1.400575)
PG_266A_LongLat <- c(103.897793,1.405382)
PG_613C_LongLat <- c(103.908573,1.404082)
PG_624C_LongLat <- c(103.911212,1.399853)
Punggol_LongLat <- rbind(PG_103C_LongLat,PG_199C_LongLat,PG_266A_LongLat,PG_613C_LongLat,PG_624C_LongLat) %>% as.data.frame()

# Freight Links E-Logistic Technopark 30 TuasAve 10 S(639150)	Latitude : 1.332937, Longitude : 103.650253
# Blk 199C Punggol Field S(823199) Latitude : 1.400575, Longitude : 103.905878
# Blk 274C PUNGGOL PLACE S(823274) Latitude : 1.403054, Longitude : 103.902472
# Blk 613C Punggol Drive S(823613) Latitude : 1.404082, Longitude : 103.908573
Receiver_Tuas_LongLat <- c(103.650253,1.332937)
Receiver_199C_LongLat <- c(103.905878,1.400575)
Receiver_274C_LongLat <- c(103.902472,1.403054)
Receiver_613C_LongLat <- c(103.908573,1.404082)

PG_103C_Distance=round(as.numeric(distm(PG_103C_LongLat,Receiver_199C_LongLat,fun = distHaversine)))
PG_199C_Distance=round(as.numeric(distm(PG_199C_LongLat,Receiver_199C_LongLat,fun = distHaversine)))
PG_266A_Distance=round(as.numeric(distm(PG_266A_LongLat,Receiver_274C_LongLat,fun = distHaversine)))
PG_613C_Distance=round(as.numeric(distm(PG_613C_LongLat,Receiver_613C_LongLat,fun = distHaversine)))
PG_624C_Distance=round(as.numeric(distm(PG_624C_LongLat,Receiver_613C_LongLat,fun = distHaversine)))
PG_Distance <- c(PG_103C_Distance,PG_199C_Distance,PG_266A_Distance,PG_613C_Distance,PG_624C_Distance)

Tuas_Customers <- read_excel("/srv/shiny-server/DataAnalyticsPortal/data/Tuas_Customers.xlsx",1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude")

Tuas <- inner_join(Tuas_Customers,HourlyIndexReadingRate_Tuas_Customers,by="MeterSerialNumber") %>%
  dplyr::group_by(customer,longitude,latitude) %>%
  dplyr::summarise(ReadingRate=round(mean(IndexReadingRate),2)) %>% as.data.frame()
Tuas$longitude <- as.numeric(Tuas$longitude)
Tuas$latitude <- as.numeric(Tuas$latitude)
Tuas <- as.data.frame(Tuas)

if (nrow(Tuas)==0) {
  Tuas <- Tuas_Customers %>% dplyr::select_("customer","longitude","latitude")
  Tuas$ReadingRate <- rep(0,nrow(Tuas_Customers))
}

Punggol <- data.frame(customer = c(blocks),
                      longitude = c(Punggol_LongLat$V1),
                      latitude = c(Punggol_LongLat$V2),
                      ReadingRate=ReadingRate_blocks)
rownames(Punggol) <- c(seq(1,length(ReadingRate_blocks)))

PunggolTuas <- rbind(Punggol,Tuas)
PunggolTuas <- as.data.frame(PunggolTuas)

PunggolTuas <- PunggolTuas %>% mutate(ReadingRatesCat=ifelse(ReadingRate < 70,'red',
                                                      ifelse(ReadingRate >= 70 & ReadingRate < 90,'orange',
                                                      ifelse(ReadingRate >= 90, 'green', NA))))

ReadingRateIcons <- awesomeIconList(
  green = makeAwesomeIcon(icon='user', library='fa', markerColor = 'green'),
  orange = makeAwesomeIcon(icon='user', library='fa', markerColor = 'orange'),
  red = makeAwesomeIcon(icon='user', library='fa', markerColor = 'red'))

## below is for the GDC to plot the Plotly chart
Tuas_CustomersDistance <- read_excel("/srv/shiny-server/DataAnalyticsPortal/data/Tuas_Customers.xlsx",1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude") %>%
  dplyr::mutate(Distance=round(as.numeric(distm(Receiver_Tuas_LongLat,cbind(longitude,latitude),fun = distHaversine)))) %>% 
  dplyr::select_("customer","Distance") %>%
  as.data.frame() %>% unique()
## distance is in meter

Punggol_DistanceReadingRate <- data.frame(customer = c(blocks),
                                           ReadingRate=ReadingRate_blocks,
                                           Distance=PG_Distance)

Tuas_DistanceReadingRate <- inner_join(Tuas,Tuas_CustomersDistance,by="customer") %>% 
                            dplyr::select_("customer","ReadingRate","Distance") %>%
                            unique()

PunggolTuas_DistanceReadingRate <- rbind(Punggol_DistanceReadingRate,Tuas_DistanceReadingRate)
PunggolTuas_DistanceReadingRate$site <- c(rep("Punggol",5),rep("Tuas",30))
 
Updated_DateTime_Dashboard <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")
  
save(weekly_consumption_last10weeks,
     LeakVolumePerDay_last10days,
     LeakInformation_DT,
     overconsumption_alarm_past10weeks, 
     PunggolNetConsumptionOutput_DirectIndirect,
     YuhuaNetConsumptionOutput_DirectIndirect,
    
     WeeklyLPCD_OnOffline,
     OnlineCustomers,OfflineCustomers,
     OverallQuantities,
     
     AverageHourlyReadingRate_Punggol,
     AverageHourlyReadingRate_Yuhua,
     Punggol_Real_Vacant_ServicePoint_Sn,Occupied_HH_zero_consumption,
     QuantityNoDataMeter,PunggolTuas,ReadingRateIcons,
     PunggolTuas_DistanceReadingRate,
     
     Updated_DateTime_Dashboard,
     
     file="/srv/shiny-server/DataAnalyticsPortal/data/Dashboard.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Dashboard successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
