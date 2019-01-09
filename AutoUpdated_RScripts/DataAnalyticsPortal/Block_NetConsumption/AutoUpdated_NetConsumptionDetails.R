rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,RPostgreSQL,stringr,ISOweek,lubridate,data.table,tidyr,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today_week <- gsub("-W","_",str_sub(date2ISOweek(today()),end = -3)) # convert date to week

# WeeklyPunggolConsumption.csv from AutoUpdated_ConsumptionIndexDownload.R
WeeklyPunggolConsumption <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/WeeklyPunggolConsumption.csv") %>% 
                            dplyr::filter(week!=today_week)

IndirectMasterMeters <- DirectMasterMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()

Indirect_MasterMeters <- Direct_MasterMeters <-list()
Indirect_SubMeters <- Direct_SubMeters <- list()

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")

## to derive NetConsumption from WeeklyPunggolConsumptionCSV for each block
## need to link each service_point_sn to which floor they are
WeeklyConsumption <- inner_join(WeeklyPunggolConsumption,servicepoint,
                                by=c("service_point_sn","block","meter_type")) %>%
                                dplyr::select_("block","floor","service_point_sn","supply","meter_type","meter_sn",
                                               "week","LastDayofWeek","WeeklyConsumption")

all_blocks <- sort(unique(WeeklyConsumption$block))
blocks <- all_blocks[1:length(all_blocks)-1]
last_block <- all_blocks[length(all_blocks)]

WeeklyConsumption_Master <- WeeklyConsumption %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS"))
WeeklyConsumption_SUB <- WeeklyConsumption %>%
  dplyr::filter(meter_type=="SUB")

for(j in 1:length(blocks)) {
  IndirectMasterMeters[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Indirect_MasterMeters[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  DirectMasterMeters[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Direct_MasterMeters[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  IndirectSubMeters[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Indirect_SubMeters[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  DirectSubMeters[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Direct_SubMeters[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
}

# for PG_B5, last_block
IndirectMasterMeters[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_MasterMeters[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectMasterMeters[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_MasterMeters[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectSubMeters[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectSubMeters[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_SubMeters[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectMasterMeters_DF <- rbindlist(IndirectMasterMeters) 
DirectMasterMeters_DF <- rbindlist(DirectMasterMeters) 
IndirectSubMeters_DF <- rbindlist(IndirectSubMeters) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
                        dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()
DirectSubMeters_DF <- rbindlist(DirectSubMeters) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
                      dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()
MasterMeters_DF <- rbind(IndirectMasterMeters_DF,DirectMasterMeters_DF)
SubMeters_DF <- rbind(IndirectSubMeters_DF,DirectSubMeters_DF)

MasterSubMeters_DF <- rbind(MasterMeters_DF,
                            SubMeters_DF[,c("block","LastDayofWeek","supply","meter_type","meter_sn","WeeklyConsumption")]) 

## this is to get the NetConsumption and NetConsumptionRate
IndirectMaster_Meters <- rbindlist(Indirect_MasterMeters) %>% dplyr::rename(IndirectMaster=WeeklyConsumption)
DirectMaster_Meters <- rbindlist(Direct_MasterMeters) %>% dplyr::rename(DirectMaster=WeeklyConsumption)
IndirectSub_Meters <- rbindlist(Indirect_SubMeters) %>% dplyr::group_by(block,week) %>% 
                      dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>%
                      dplyr::rename(IndirectSub=WeeklyConsumption) %>% as.data.frame()
DirectSub_Meters <- rbindlist(Direct_SubMeters) %>% dplyr::group_by(block,week) %>% 
                    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>%
                    dplyr::rename(DirectSub=WeeklyConsumption) %>% as.data.frame()
Master_Meters <- cbind(IndirectMaster_Meters,DirectMaster_Meters[,c("DirectMaster")])
Sub_Meters <- cbind(IndirectSub_Meters,DirectSub_Meters[,c("DirectSub")])
colnames(Sub_Meters)[4] <- "DirectSub"
MasterSub_Meters <- cbind(Master_Meters,Sub_Meters[,c("IndirectSub","DirectSub")]) %>%
  dplyr::mutate(IndirectNetConsumption=IndirectMaster-IndirectSub,
                DirectNetConsumption=DirectMaster-DirectSub,
                IndirectNetConsumptionRate=round((IndirectNetConsumption/IndirectMaster)*100,1),
                DirectNetConsumptionRate=round((DirectNetConsumption/DirectMaster)*100,1),
                TotalMaster=IndirectMaster+DirectMaster,
                TotalNetConsumption=IndirectNetConsumption+DirectNetConsumption,
                TotalNetConsumptionRate=round((TotalNetConsumption/TotalMaster)*100,1)) %>%
  dplyr::select_("block","LastDayofWeek","IndirectMaster","IndirectSub","IndirectNetConsumption","IndirectNetConsumptionRate",
                 "DirectMaster","DirectSub","DirectNetConsumption","DirectNetConsumptionRate",
                 "TotalMaster","TotalNetConsumption","TotalNetConsumptionRate")

SubMetersCountPerWeekPerBlock <- SubMeters_DF %>% dplyr::group_by(block,week) %>%
                                 dplyr::summarise(MeterCount=n())

MasterSubMeters_Counts <- cbind(MasterSub_Meters,SubMetersCountPerWeekPerBlock[3])
SUB_CountsPerBlock <- rep(c(90,92,118,99,129),each=nrow(MasterSub_Meters)/length(all_blocks)) %>% as.data.frame()
colnames(SUB_CountsPerBlock) <- "SupposedCount"

MasterSubMetersCounts <- cbind(MasterSubMeters_Counts,SUB_CountsPerBlock) %>%
                              dplyr::mutate(DiffCount=MeterCount-SupposedCount) %>%
                              dplyr::mutate(IndirectSub=ifelse(DiffCount!=0,NA,IndirectSub)) %>%
                              dplyr::mutate(IndirectNetConsumption=ifelse(DiffCount!=0,NA,IndirectNetConsumption)) %>%
                              dplyr::mutate(IndirectNetConsumptionRate=ifelse(DiffCount!=0,NA,IndirectNetConsumptionRate)) %>%
                              dplyr::mutate(DirectSub=ifelse(DiffCount!=0,NA,DirectSub)) %>%
                              dplyr::mutate(DirectNetConsumption=ifelse(DiffCount!=0,NA,DirectNetConsumption)) %>%
                              dplyr::mutate(DirectNetConsumptionRate=ifelse(DiffCount!=0,NA,DirectNetConsumptionRate)) %>%
                              dplyr::mutate(TotalNetConsumption=ifelse(DiffCount!=0,NA,TotalNetConsumption)) %>%
                              dplyr::mutate(TotalNetConsumptionRate=ifelse(DiffCount!=0,NA,TotalNetConsumptionRate)) 

WeeklyNetConsumptionCSV <- MasterSubMetersCounts[,c(1:13)]

WeeklyNetConsumptionDate <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek") %>% unique()

WeeklyNetConsumptionCSV_TotalNetConsumption <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek","block","TotalNetConsumption")
WeeklyNetConsumptionCSV_TotalNetConsumption_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumption,block,TotalNetConsumption)
WeeklyNetConsumptionCSV_TotalNetConsumption_wide["LastDayofWeek"] <- NULL
WeeklyNetConsumption_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumption_wide,order.by=as.Date(WeeklyNetConsumptionDate$LastDayofWeek))

WeeklyNetConsumptionCSV_TotalNetConsumptionRate <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek","block","TotalNetConsumptionRate")
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumptionRate,block,TotalNetConsumptionRate)
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide["LastDayofWeek"] <- NULL
WeeklyNetConsumptionRate_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide,order.by=as.Date(WeeklyNetConsumptionDate$LastDayofWeek))

WeeklyNetConsumption <- MasterSubMetersCounts %>% dplyr::select_("LastDayofWeek","block",
                                                                  "IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")
WeeklyNetConsumption <- WeeklyNetConsumption %>% dplyr::arrange(desc(LastDayofWeek))
WeeklyNetConsumption_NA <- WeeklyNetConsumption %>% 
                           dplyr::mutate(IndirectNetConsumptionRate=ifelse(is.na(IndirectNetConsumptionRate),"NA",IndirectNetConsumptionRate),
                                         DirectNetConsumptionRate=ifelse(is.na(DirectNetConsumptionRate),"NA",DirectNetConsumptionRate),
                                         TotalNetConsumptionRate=ifelse(is.na(TotalNetConsumptionRate),"NA",TotalNetConsumptionRate))

WeeklyNetConsumptionCSV <- WeeklyNetConsumptionCSV %>% dplyr::arrange(desc(LastDayofWeek))

write.csv(WeeklyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.csv",row.names=FALSE)

assign_NA <- WeeklyNetConsumptionCSV[which(is.na(WeeklyNetConsumptionCSV$TotalNetConsumptionRate)),1:2]
WeeklyPunggolNetConsumptionDetails <- WeeklyPunggolConsumption %>% dplyr::group_by(block,week,supply) %>%
                                      dplyr::mutate(NetConsumption=sum(WeeklyConsumption[meter_type!="SUB"])-sum(WeeklyConsumption[meter_type=="SUB"]))
WeeklyPunggolNetConsumptionDetails_NA <- WeeklyPunggolNetConsumptionDetails %>%
                                         dplyr::mutate(NetConsumption=ifelse((block %in% assign_NA$block & LastDayofWeek %in% assign_NA$LastDayofWeek),NA,
                                                       NetConsumption)) %>% as.data.frame()
WeeklyPunggolNetConsumptionDetails_NA <- WeeklyPunggolNetConsumptionDetails_NA %>%
                                         dplyr::mutate(NetConsumption=ifelse(is.na(NetConsumption),"NA",NetConsumption))


write.csv(WeeklyPunggolNetConsumptionDetails_NA,"/srv/shiny-server/DataAnalyticsPortal/data/WeeklyPunggolNetConsumptionDetailsCSV.csv",row.names=FALSE)

Updated_DateTime_NetConsumptionDetails_Weekly <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(WeeklyNetConsumption_NA,WeeklyNetConsumption_xts,WeeklyNetConsumptionRate_xts,
     WeeklyPunggolNetConsumptionDetails_NA,
     Updated_DateTime_NetConsumptionDetails_Weekly,
     file="/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumptionDetails successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)