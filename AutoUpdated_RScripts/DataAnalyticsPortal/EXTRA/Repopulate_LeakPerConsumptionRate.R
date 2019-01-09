rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists


if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

flow <- as.data.frame(tbl(con,"flow"))
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

servicepoint <- servicepoint %>% dplyr::filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

consumption_servicepoint <- inner_join(PunggolConsumption_SUB,servicepoint,by=c("service_point_sn","block"))

leak_data <- subset(flow_servicepoint_meter, select = c("service_point_sn","block","meter_sn","min_5_flow","flow_date"))
consumption_data <- subset(consumption_servicepoint, select = c("service_point_sn","adjusted_consumption","date_consumption","block"))

all_blocks <- sort(unique(Punggol_All$block))

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

LeakVolumePerWeek <- list()
ConsumptionVolumePerWeek <- list()
LeakVolume <- list()
ConsumptionVolume <- list()

weeks_seq <- seq(4,67)
for (i in 1:length(weeks_seq))
{
  week.start <- Week.date$beg[weeks_seq[i]]
  week.end <- Week.date$end[weeks_seq[i]]
  
  LeakVolumePerWeek[[i]] <- leak_data %>% 
    dplyr::filter(date(flow_date) >= week.start & date(flow_date) <= week.end) %>%
    group_by(block) %>%
    dplyr::mutate(LeakVolumePerDay=min_5_flow*24) %>%
    dplyr::summarise(TotalLeakVolumePerWeek=sum(LeakVolumePerDay))
  
  ConsumptionVolumePerWeek[[i]] <- consumption_data %>% 
    dplyr::filter(date(date_consumption) >= week.start & date(date_consumption) <= week.end) %>%
    group_by(block) %>%
    dplyr::summarise(TotalConsumptionVolumePerWeek=sum(adjusted_consumption,na.rm = TRUE))
  
  LeakVolume[[i]] <- cbind(Date=as.data.frame(rep(week.end,5)),
                           Block=as.data.frame(all_blocks),
                           LeakVolumePerWeek[[i]]$TotalLeakVolumePerWeek)
  
  ConsumptionVolume[[i]] <- cbind(Date=as.data.frame(rep(week.end,5)),
                                  Block=as.data.frame(all_blocks),
                                  ConsumptionVolumePerWeek[[i]]$TotalConsumptionVolumePerWeek)
  
}
LeakVolumePerWeekPerBlock <- as.data.frame(rbindlist(LeakVolume))
colnames(LeakVolumePerWeekPerBlock) <- c("Date","Block","LeakVolume")
ConsumptionVolumePerWeekPerBlock <- as.data.frame(rbindlist(ConsumptionVolume))
colnames(ConsumptionVolumePerWeekPerBlock) <- c("Date","Block","ConsumptionVolume")

save(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionVolumePerWeekPerBlock.RData")

LeakConsumptionPerWeekPerBlock <- inner_join(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock,by=c("Date","Block")) %>%
  dplyr::mutate(LeakPerConsumptionRate=round((LeakVolume/ConsumptionVolume)*100,2))

LeakConsumptionPerWeekPerBlockDate <- LeakConsumptionPerWeekPerBlock %>% dplyr::select_("Date") %>% unique()

LeakVolumeList <- list()
for(i in 1:length(all_blocks)){
  LeakVolumeList[[i]] <- LeakConsumptionPerWeekPerBlock %>% filter(Block==all_blocks[i]) %>% dplyr::select_("LeakVolume")
}

LeakVolume <- as.data.frame(unlist(LeakVolumeList))
nr <- nrow(LeakVolume)
n <- nr/length(all_blocks)
LeakVolume <- as.data.frame(split(LeakVolume, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(LeakVolume) <- all_blocks
LeakVolume_xts <- xts(LeakVolume,order.by=as.Date(LeakConsumptionPerWeekPerBlockDate$Date))

LeakPerConsumptionRateList <- list()
for(i in 1:length(all_blocks)){
  LeakPerConsumptionRateList[[i]] <- LeakConsumptionPerWeekPerBlock %>% filter(Block==all_blocks[i]) %>% dplyr::select_("LeakPerConsumptionRate")
}

LeakPerConsumptionRate <- as.data.frame(unlist(LeakPerConsumptionRateList))
nr <- nrow(LeakPerConsumptionRate)
n <- nr/length(all_blocks)
LeakPerConsumptionRate <- as.data.frame(split(LeakPerConsumptionRate, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(LeakPerConsumptionRate) <- all_blocks
LeakPerConsumptionRate_xts <- xts(LeakPerConsumptionRate,order.by=as.Date(LeakConsumptionPerWeekPerBlockDate$Date))

save(LeakVolume_xts,LeakPerConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakPerConsumptionRate.RData")
write.csv(LeakConsumptionPerWeekPerBlock,"/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionPerWeekPerBlock.csv",row.names=FALSE)