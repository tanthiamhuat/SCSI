rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
# Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
# Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
# Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
# Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last30days.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/BillableMeters_IndexChecks_last30days.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/BlockWaterSavings.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/DailyLPCD.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/WeeklyLPCD.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/UnexpectedConsumption.RData")

flow <- as.data.frame(tbl(con,"flow"))
family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
family_ALL <- as.data.frame(tbl(con,"family"))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
family_servicepoint_online <- family_servicepoint %>% dplyr::filter(online_status=="ACTIVE")
meter <- as.data.frame(tbl(con,"meter"))
servicepoint_MAINBYPASS <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type")) %>%
                           filter(site=="Punggol" & meter_type %in% c("MAIN","BYPASS"))

servicepoint_SUB <- servicepoint %>% filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL
servicepoint_SUB_meter <- inner_join(servicepoint_SUB,meter,by=c("service_point_sn"="id_real_estate","meter_type"))

flow_servicepoint_SUB_meter <- inner_join(servicepoint_SUB_meter,flow,by=c("id.x"="id_service_point"))
flow_servicepoint_SUB_meter <- subset(flow_servicepoint_SUB_meter, select = c("service_point_sn","block","floor","meter_sn","site","min_5_flow","flow_date"))

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

weekly_consumption_last10weeks$week_number <- substr(weekly_consumption_last10weeks$week_number,6,7)
weekly_consumption_last10weeks$WeeklyConsumption <- round(weekly_consumption_last10weeks$WeeklyConsumption)

# Breakdown Consumption per block % (Donut chart)
# ConsumptionBreakdown <- Punggol_All %>% 
#                         dplyr::filter(meter_type=="SUB") %>%
#                         group_by(block) %>%
#                         dplyr::summarise(TotalConsumption=sum(adjusted_consumption,na.rm = TRUE)) 
# ConsumptionBreakdown$Percent <- round(ConsumptionBreakdown$TotalConsumption/sum(ConsumptionBreakdown$TotalConsumption)*100)

## -------- Leaks and Anomalies ------------- ##
# Leak Volume per day (mini chart of the last 10 days) and 
LeakVolumePerDay_last10days <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/LeakVolumePerDay.csv") %>%
  dplyr::arrange(desc(Date)) %>%
  head(10)
LeakVolumePerDay_last10days <- LeakVolumePerDay_last10days %>% 
  dplyr::mutate(Date1=paste(month.abb[month(Date)],"-",substr(Date,9,10),sep=""),
                Date2=paste(substr(Date,6,7),substr(Date,9,10),sep="/"))

# Total Quantity of opened leaks (raw number) and 
# trend (based on average quantity of opened leaks over the last 7 days versus last 30 days)
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
} else {
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
} else {
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
} else {
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

load('/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.RData')
colnames(WeeklyNetConsumption) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption","DirectMainMeters","DirectNetConsumption",
                                    "TotalMainMeters","TotalNetConsumption","TotalNetConsumptionRate")
WeeklyNetConsumptionRateDirectIndirect <- WeeklyNetConsumption %>% 
                                          dplyr::mutate(DirectNetConsumptionRate=round(DirectNetConsumption/DirectMainMeters*100,2),
                                                        IndirectNetConsumptionRate=round(IndirectNetConsumption/IndirectMainMeters*100,2)) %>%
                                          dplyr::select_("Date","Block","DirectNetConsumptionRate","IndirectNetConsumptionRate") %>%
                                          head(5)
WeeklyNetConsumptionRateDirectIndirect$Block <- as.character(WeeklyNetConsumptionRateDirectIndirect$Block)

leak_alarm_Open_MAINBYPASS <- as.data.frame(tbl(con,"leak_alarm")) %>% dplyr::filter(status=="Open" & site=="Punggol" & meter_type %in% c("MAIN","BYPASS"))

MAINBYPASS_CF_Leak <- MAINBYPASS_CF %>%
                      dplyr::mutate(Leak=ifelse(service_point_sn %in% leak_alarm_Open_MAINBYPASS$service_point_sn,1,0)) %>%
                      dplyr::group_by(block) %>%
                      dplyr::summarise(TotalLeak=sum(Leak))

WeeklyNetConsumptionRateDirectIndirect_Leak <- inner_join(WeeklyNetConsumptionRateDirectIndirect,MAINBYPASS_CF_Leak,by=c("Block"="block")) 

keycol <- "NetConsumption"
valuecol <- "NetConsumptionRate"
gathercols <- c("IndirectNetConsumptionRate", "DirectNetConsumptionRate")

WeeklyNetConsumptionRateDirectIndirect_Leak_Long <- gather_(WeeklyNetConsumptionRateDirectIndirect_Leak, keycol, valuecol, gathercols) %>%
                                                    dplyr::arrange(Block) %>% dplyr::select_("NetConsumptionRate","TotalLeak")
Blocks_CommonFittings_NetConsumptionRate <- cbind(Blocks_CommonFittings,WeeklyNetConsumptionRateDirectIndirect_Leak_Long)

Blocks_CommonFittings_NetConsumptionRate_Status <- Blocks_CommonFittings_NetConsumptionRate %>% 
                                                   dplyr::mutate(Status=ifelse(TotalLeak==0 & Indirect==0 & NetConsumptionRate < 4,"Green",
                                                                        ifelse(TotalLeak==0 & Indirect==0 & NetConsumptionRate >=4 & NetConsumptionRate <=7,"Orange",
                                                                        ifelse(TotalLeak>=0 & Indirect==0 & NetConsumptionRate > 7 ,"Red",
                                                                        ifelse(TotalLeak==0 & Indirect==1 & NetConsumptionRate < 5,"Green",
                                                                        ifelse(TotalLeak==0 & Indirect==1 & NetConsumptionRate >=5 & NetConsumptionRate <=10,"Orange",
                                                                        ifelse(TotalLeak>=0 & Indirect==1 & NetConsumptionRate > 10 ,"Red",0)))))))

NetConsumptionOutput <- Blocks_CommonFittings_NetConsumptionRate_Status %>%
                        dplyr::select_("block","Indirect","Status")
NetConsumptionOutput_DirectIndirect <- spread(NetConsumptionOutput, Indirect, Status)
colnames(NetConsumptionOutput_DirectIndirect) <- c("Block","Direct","Indirect")

## -------- Water Savings ------------- ##
WaterSaved_LeakAlarm <- sum(BlockWaterSavingsValues$Savings_w_LeakAlarm,na.rm=TRUE)
WaterSavedLeakAlarm_Percent <- round(WaterSaved_LeakAlarm/sum(BlockWaterSavingsValues$OverallConsumption)*100,2)
WaterSaved_Gamification <- sum(BlockWaterSavingsValues$Savings_w_Gamification,na.rm=TRUE)
WaterSavedGamification_Percent <- round(WaterSaved_Gamification/sum(BlockWaterSavingsValues$ConsumptionAfterGamificationStarted)*100,2)

## -------- Customer Engagement ------------- ##
OnlineCustomers <- family %>% dplyr::filter(online_status=="ACTIVE")
OfflineCustomers <- family %>% dplyr::filter(online_status=="INACTIVE")

OnlineCustomers_ServicePoint <- inner_join(OnlineCustomers,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","num_house_member")
OfflineCustomers_ServicePoint <- inner_join(OfflineCustomers,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","num_house_member")

WeeklyConsumption_Online <- WeeklyConsumption %>% dplyr::filter(service_point_sn %in% OnlineCustomers_ServicePoint$service_point_sn)
WeeklyConsumption_Offline <- WeeklyConsumption %>% dplyr::filter(service_point_sn %in% OfflineCustomers_ServicePoint$service_point_sn)

WeeklyLPCD_Online <- WeeklyConsumption_Online %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD_Online=round(TotalWeeklyConsumption/(TotalHH*7))) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD_Online") %>% 
  dplyr::filter(Year=="2017" & Week >=23) %>% as.data.frame()
WeeklyLPCD_Online <- WeeklyLPCD_Online[1:(nrow(WeeklyLPCD_Online)-1),]

WeeklyLPCD_Offline <- WeeklyConsumption_Offline %>% 
  dplyr::mutate(Year=substr(yearweek,1,4),Week=substr(yearweek,6,8)) %>%
  dplyr::group_by(Year,Week,yearweek) %>%
  dplyr::summarise(TotalWeeklyConsumption=sum(WeeklyConsumption),TotalHH=sum(num_house_member)) %>%
  dplyr::mutate(WeeklyLPCD_Offline=round(TotalWeeklyConsumption/(TotalHH*7))) %>% 
  dplyr::select_("Year","Week","WeeklyLPCD_Offline") %>% 
  dplyr::filter(Year=="2017" & Week >=23)
WeeklyLPCD_Offline <- WeeklyLPCD_Offline[1:(nrow(WeeklyLPCD_Offline)-1),]

WeeklyLPCD_OnOffline <- inner_join(WeeklyLPCD_Online,WeeklyLPCD_Offline,by=c("Year","Week"))

## -------- Overall Summary ------------- ##
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
Tuas_Main <- servicepoint_meter %>% dplyr::filter(site=="Tuas" & meter_type=="MAIN" & status=="ACTIVE")
Tuas_ByPass <- servicepoint_meter %>% dplyr::filter(site=="Tuas" & meter_type=="BYPASS" & status=="ACTIVE")

Punggol_RT01 <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & room_type=="HDB01" & status=="ACTIVE")
Punggol_RT02 <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & room_type=="HDB02" & status=="ACTIVE")
Punggol_RT03 <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & room_type=="HDB03" & status=="ACTIVE")
Punggol_RT04 <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & room_type=="HDB04" & status=="ACTIVE")
Punggol_RT05 <- servicepoint_meter %>% dplyr::filter(site=="Punggol" & room_type=="HDB05" & status=="ACTIVE")

Punggol_Occupied <- family_servicepoint %>% dplyr::filter(site=="Punggol" & status=="ACTIVE")
Punggol_Vacant <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id=="EMPTY" & status=="VACANT")
## need to check whether those id_service_point with status=VACANT has also status=ACTIVE which has later move_in_date
## e.g id_service_point=213,222,492

Punggol_Not_Vacant <-  as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(id_service_point %in% Punggol_Vacant$id_service_point) %>%
  dplyr::filter(status=="ACTIVE")
Punggol_Real_Vacant <- setdiff(Punggol_Vacant$id_service_point,Punggol_Not_Vacant$id_service_point)
Punggol_Real_Vacant_ServicePoint_Sn <- servicepoint %>% dplyr::filter(id %in% Punggol_Real_Vacant) %>% dplyr::select_("service_point_sn")

consumption <- tbl(con, "consumption")
consumption_last30days <- consumption %>% filter(date(date_consumption)>=last30days) %>% collect()

PunggolConsumption_SUB_last30days <- Punggol_2017 %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type)) & 
                  service_point_sn %in% familyALL_servicepoint$service_point_sn & site=="Punggol") %>%
  select(service_point_sn,adjusted_consumption,adjusted_date) %>%
  dplyr::filter(date(adjusted_date)>=last30days) %>%
  group_by(service_point_sn) %>%
  dplyr::summarise(TotalConsumption=sum(adjusted_consumption,na.rm=TRUE))

Occupied_HH_zero_consumption <- dplyr::setdiff(PunggolConsumption_SUB_last30days[which(PunggolConsumption_SUB_last30days$TotalConsumption==0),1],
                                               Punggol_Real_Vacant_ServicePoint_Sn)

OverallDataSummary = data.table(ItemDescription=c("Qty (Main+ByPass) Meters","Qty SUB Meters",
                                                  "Qty of households (active customers)",
                                                  "Qty Occupied Household","Qty Occupied HH (zero consumption)",
                                                  "Qty Vacant Household","Qty Vacant HH (non-zero consumption)"),
                                Punggol=c(nrow(Punggol_Main)+nrow(Punggol_ByPass),nrow(Punggol_Sub),
                                          nrow(Punggol_RT01)+nrow(Punggol_RT02)+nrow(Punggol_RT03)+nrow(Punggol_RT04)+nrow(Punggol_RT05),
                                          nrow(Punggol_Occupied),nrow(Occupied_HH_zero_consumption),
                                          length(Punggol_Real_Vacant),nrow(unexpected_consumption)),
                                Tuas=c(nrow(Tuas_Main)+nrow(Tuas_ByPass),rep(NA,6)),
                                Yuhua=rep(NA,7))

## -------- Data Quality ------------- ##
AverageHourlyReadingRate_Punggol <- HourlyIndexReadingRate_Punggol %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

AverageHourlyReadingRate_Yuhua <- NA

QuantityNoDataMeter <- BillableMeters[nrow(BillableMeters),ncol(BillableMeters)]

AverageHourlyReadingRate <- HourlyIndexReadingRate_Block %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

blocks <- sort(unique(HourlyIndexReadingRate_Block$block))
ReadingRate_blocks <- list()
for (i in 1:length(blocks)){
  ReadingRate_blocks[[i]] <- AverageHourlyReadingRate %>% filter(block==blocks[i]) %>% dplyr::select_("AverageHourlyReadingRate")
}
ReadingRate_blocks <- c(unlist(ReadingRate_blocks))

## http://www.latlong.net/convert-address-to-lat-long.html
## PG_B1, 103C Edgefield Plains, Lat=1.397414,Lon=103.904557
## PG_B2, 199C Punggol Field, Lat=1.400575,Lon=103.905878
## PG_B3, 266A Punggol Way,Lat=1.405382,Lon=103.897793
## PG_B4, 613C, Punggol Drive, Lat=1.404176,Lon=103.908394
## PG_B5, 624C Punggol Central, Lat=1.399853,Lon=103.911212
## Yuhua, Latitude=1.3245370, Longitude=103.7425669

Tuas_Customers <- read_excel("/srv/shiny-server/DataAnalyticsPortal/data/Tuas_Customers.xlsx",1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude")

Tuas <- inner_join(Tuas_Customers,HourlyIndexReadingRate_Tuas_Customers,by="MeterSerialNumber") %>%
  dplyr::group_by(customer,longitude,latitude) %>%
  dplyr::summarise(ReadingRate=round(mean(IndexReadingRate),2)) %>% as.data.frame()
Tuas$longitude <- as.numeric(Tuas$longitude)
Tuas$latitude <- as.numeric(Tuas$latitude)
Tuas <- as.data.frame(Tuas)

Punggol <- data.frame(customer = c(blocks),
                      longitude = c(103.904557,103.905878,103.897793,103.908394,103.911212),
                      latitude = c(1.397414,1.400575,1.405382,1.404176,1.399853),
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

Updated_DateTime_DashBoard <- paste("Last Updated on",now())
  
save(weekly_consumption_last10weeks,
     LeakVolumePerDay_last10days,
     LeakInformation_DT,
     overconsumption_alarm_past10weeks, 
     NetConsumptionOutput_DirectIndirect,
    
     WaterSaved_LeakAlarm,
     WaterSaved_Gamification,
     WaterSavedLeakAlarm_Percent,
     WaterSavedGamification_Percent,
     
     WeeklyLPCD_OnOffline,
     OnlineCustomers,OfflineCustomers,
     OverallDataSummary,
     
     AverageHourlyReadingRate_Punggol,
     AverageHourlyReadingRate_Yuhua,
     QuantityNoDataMeter,PunggolTuas,ReadingRateIcons,
     
     Updated_DateTime_DashBoard,
     
     file="/srv/shiny-server/DataAnalyticsPortal/data/DashBoard.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Dashboard successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
