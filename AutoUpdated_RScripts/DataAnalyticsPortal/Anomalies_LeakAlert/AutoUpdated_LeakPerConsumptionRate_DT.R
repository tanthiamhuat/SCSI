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
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

flow <- as.data.frame(tbl(con,"flow"))
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst")

servicepoint <- servicepoint %>% dplyr::filter(!is.na(floor) & !is.na(unit)) # only Punggol HH units, exclude main meters, include HDBCD + HL

servicepoint_meter <- inner_join(servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

flow_servicepoint_meter <- inner_join(servicepoint_meter,flow,by=c("id.x"="id_service_point"))

leak_data <- subset(flow_servicepoint_meter, select = c("service_point_sn","block","meter_sn","min_5_flow","flow_date"))
consumption_data <- subset(consumption_last6months_servicepoint, select = c("service_point_sn","adjusted_consumption","date_consumption","block"))

all_blocks <- sort(unique(consumption_last6months_servicepoint$block))

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionVolumePerWeekPerBlock.RData")

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
LeakVolumePerWeekPerBlock <- unique(LeakVolumePerWeekPerBlock)

colnames(ConsumptionVolume_New) <- c("Date","Block","ConsumptionVolume")
ConsumptionVolumePerWeekPerBlock = rbind(ConsumptionVolumePerWeekPerBlock,ConsumptionVolume_New)
ConsumptionVolumePerWeekPerBlock <- unique(ConsumptionVolumePerWeekPerBlock)

save(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock, file="/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionVolumePerWeekPerBlock.RData")

LeakConsumptionPerWeekPerBlock <- inner_join(LeakVolumePerWeekPerBlock,ConsumptionVolumePerWeekPerBlock, by=c("Date","Block")) %>%
                                  dplyr::mutate(LeakPerConsumptionRate=round(LeakVolume/ConsumptionVolume*100,2))

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

Updated_DateTime_LeakRatePerWeek <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(LeakVolume_xts,LeakPerConsumptionRate_xts,Updated_DateTime_LeakRatePerWeek,file="/srv/shiny-server/DataAnalyticsPortal/data/LeakPerConsumptionRate.RData")
write.csv(LeakConsumptionPerWeekPerBlock,"/srv/shiny-server/DataAnalyticsPortal/data/LeakConsumptionPerWeekPerBlock.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LeakPerConsumptionRate successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)

