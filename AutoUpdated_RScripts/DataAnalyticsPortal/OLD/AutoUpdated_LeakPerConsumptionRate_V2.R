## To tabulate the percentage of leak volume/total consumption per block per week
## To run on every Monday
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,xts)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

flow <- as.data.frame(tbl(con,"flow"))
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

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

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionVolumePerWeekPerBlock.RData")

all_blocks <- sort(unique(Punggol_All$block))

weeknumber <- as.numeric(strftime(today(),format="%W")) 
i <- weeknumber-1
if (i <10) {i <- paste(0,i,sep="")}
WeekNumber=paste(year(today()),"_",i,sep="") 
if (i =="00") {
  i <- 52
  WeekNumber=paste(year(today())-1,"_",i,sep="")
}
week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
week.end <- Week.date$end[which(Week.date$week==WeekNumber)]

  LeakVolumePerWeek <- leak_data %>% 
                       dplyr::filter(date(flow_date) >= week.start & date(flow_date) <= week.end) %>%
                       group_by(block) %>%
                       dplyr::mutate(LeakVolumePerDay=min_5_flow*24) %>%
                       dplyr::summarise(TotalLeakVolumePerWeek=sum(LeakVolumePerDay))
  
  ConsumptionVolumePerWeek <- consumption_data %>% 
                              dplyr::filter(date(date_consumption) >= week.start & date(date_consumption) <= week.end) %>%
                              group_by(block) %>%
                              dplyr::summarise(TotalConsumptionVolumePerWeek=sum(adjusted_consumption,na.rm = TRUE))
  
  LeakVolume_New <- cbind(Date=as.data.frame(rep(week.end,5)),
                           Block=as.data.frame(all_blocks),
                           LeakVolumePerWeek$TotalLeakVolumePerWeek)
  
  ConsumptionVolume_New <- cbind(Date=as.data.frame(rep(week.end,5)),
                                  Block=as.data.frame(all_blocks),
                                  ConsumptionVolumePerWeek$TotalConsumptionVolumePerWeek)

colnames(LeakVolume_New) <- c("Date","Block","LeakVolume")
LeakVolumePerWeekPerBlock = rbind(LeakVolumePerWeekPerBlock,LeakVolume_New)

colnames(ConsumptionVolume_New) <- c("Date","Block","ConsumptionVolume")
ConsumptionVolumePerWeekPerBlock = rbind(ConsumptionVolumePerWeekPerBlock,ConsumptionVolume_New)

save(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionVolumePerWeekPerBlock.RData")

LeakConsumptionPerWeekPerBlock <- inner_join(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock, by=c("Date","Block")) %>%
                                  dplyr::mutate(LeakPerConsumptionRate=round(LeakVolume/ConsumptionVolume*100,2))

LeakVolume_PG_B1 <- LeakVolumePerWeekPerBlock %>% dplyr::filter(Block=="PG_B1")
LeakVolume_PG_B1 <- xts(LeakVolume_PG_B1$LeakVolume,LeakVolume_PG_B1$Date)
LeakVolume_PG_B2 <- LeakVolumePerWeekPerBlock %>% dplyr::filter(Block=="PG_B2")
LeakVolume_PG_B2 <- xts(LeakVolume_PG_B2$LeakVolume,LeakVolume_PG_B2$Date)
LeakVolume_PG_B3 <- LeakVolumePerWeekPerBlock %>% dplyr::filter(Block=="PG_B3")
LeakVolume_PG_B3 <- xts(LeakVolume_PG_B3$LeakVolume,LeakVolume_PG_B3$Date)
LeakVolume_PG_B4 <- LeakVolumePerWeekPerBlock %>% dplyr::filter(Block=="PG_B4")
LeakVolume_PG_B4 <- xts(LeakVolume_PG_B4$LeakVolume,LeakVolume_PG_B4$Date)
LeakVolume_PG_B5 <- LeakVolumePerWeekPerBlock %>% dplyr::filter(Block=="PG_B5")
LeakVolume_PG_B5 <- xts(LeakVolume_PG_B5$LeakVolume,LeakVolume_PG_B5$Date)

LeakVolume_xts <- cbind(LeakVolume_PG_B1,LeakVolume_PG_B2,
                        LeakVolume_PG_B3,LeakVolume_PG_B4,LeakVolume_PG_B5)
colnames(LeakVolume_xts)[1:5] <- all_blocks 

LeakPerConsumptionRate_PG_B1 <- LeakConsumptionPerWeekPerBlock %>% dplyr::filter(Block=="PG_B1")
LeakPerConsumptionRate_PG_B1 <- xts(LeakPerConsumptionRate_PG_B1$LeakPerConsumptionRate,LeakPerConsumptionRate_PG_B1$Date)
LeakPerConsumptionRate_PG_B2 <- LeakConsumptionPerWeekPerBlock %>% dplyr::filter(Block=="PG_B2")
LeakPerConsumptionRate_PG_B2 <- xts(LeakPerConsumptionRate_PG_B2$LeakPerConsumptionRate,LeakPerConsumptionRate_PG_B2$Date)
LeakPerConsumptionRate_PG_B3 <- LeakConsumptionPerWeekPerBlock %>% dplyr::filter(Block=="PG_B3")
LeakPerConsumptionRate_PG_B3 <- xts(LeakPerConsumptionRate_PG_B3$LeakPerConsumptionRate,LeakPerConsumptionRate_PG_B3$Date)
LeakPerConsumptionRate_PG_B4 <- LeakConsumptionPerWeekPerBlock %>% dplyr::filter(Block=="PG_B4")
LeakPerConsumptionRate_PG_B4 <- xts(LeakPerConsumptionRate_PG_B4$LeakPerConsumptionRate,LeakPerConsumptionRate_PG_B4$Date)
LeakPerConsumptionRate_PG_B5 <- LeakConsumptionPerWeekPerBlock %>% dplyr::filter(Block=="PG_B5")
LeakPerConsumptionRate_PG_B5 <- xts(LeakPerConsumptionRate_PG_B5$LeakPerConsumptionRate,LeakPerConsumptionRate_PG_B5$Date)

LeakPerConsumptionRate_xts <- cbind(LeakPerConsumptionRate_PG_B1,LeakPerConsumptionRate_PG_B2,
                                    LeakPerConsumptionRate_PG_B3,LeakPerConsumptionRate_PG_B4,LeakPerConsumptionRate_PG_B5)
colnames(LeakPerConsumptionRate_xts)[1:5] <- all_blocks

save(LeakVolume_xts,LeakPerConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakPerConsumptionRate.RData")
write.csv(LeakConsumptionPerWeekPerBlock,"/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionPerWeekPerBlock.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakPerConsumptionRate_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

