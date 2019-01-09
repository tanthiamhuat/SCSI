gc()
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek,fst,xts,timeDate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

thismonth <- as.character(timeLastDayInMonth(today()))

consumption_last12months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last12months_servicepoint.fst",as.data.table = TRUE)

Punggol_SUB <- consumption_last12months_servicepoint[site=="Punggol" & meter_type=="SUB" & adjusted_consumption != "NA"] # contain data on sub meter, including childcare

all_blocks_PG <- sort(unique(Punggol_SUB$block))  
blocks_PG <- all_blocks_PG[1:length(all_blocks_PG)-1]
last_block_PG <- all_blocks_PG[length(all_blocks_PG)]

family <- as.data.table(tbl(con,"family"))[address!="FAKE FAMILY" & status=="ACTIVE" & id_service_point!='601'] ## exclude AHL
servicepoint <- as.data.table(tbl(con,"service_point"))[service_point_sn !="3100507837M" & service_point_sn != "3100507837B" & service_point_sn != "3100660792"]
meter <- as.data.table(tbl(con,"meter"))[status=="ACTIVE" & id_real_estate!="3100660792"] # exclude AHL

setkey(family,id_service_point)
setkey(servicepoint,id)

# perform the join, eliminating not matched rows from Right
family_servicepoint <- family[servicepoint, nomatch=0]

setkey(family_servicepoint,service_point_sn)
setkey(meter,id_real_estate)

family_servicepoint_meter <- family_servicepoint[meter,nomatch=0]
## includes all MAIN, BYPASS,SUB meters from Punggol, Yuhua and Tuas, exclude AHL

family_servicepoint_meter_supply_PG <- family_servicepoint_meter[site=="Punggol"]

family_servicepoint_meter_supply_PG <- family_servicepoint_meter_supply_PG[,supply:=ifelse(block %in% blocks_PG & meter_type=="SUB" & floor <="#04","Direct",
                                                                                    ifelse(block %in% blocks_PG & meter_type=="SUB" & floor >="#05","Indirect",
                                                                                    ifelse(block %in% last_block_PG & meter_type=="SUB" & floor <="#05","Direct",
                                                                                    ifelse(block %in% last_block_PG & meter_type=="SUB" & floor >="#06","Indirect",
                                                                                    ifelse(meter_type!="SUB" & substr(meter_sn,1,2) %in% c("DP","DM","DN","SQ"),"Direct",
                                                                                    ifelse(meter_type!="SUB" & substr(meter_sn,1,2) %in% c("EP","SM","SL"),"Indirect",0))))))]

family_servicepoint_meter_supply_PG <- family_servicepoint_meter_supply_PG[, c("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","move_in_date","move_out_date")]

setkey(consumption_last12months_servicepoint,service_point_sn,meter_type,block,site)

consumption_family_servicepoint_meter_supply_MAINBYPASSSUB <- consumption_last12months_servicepoint[family_servicepoint_meter_supply_PG,nomatch=0]

consumption_family_servicepoint_meter_supply_MAINBYPASS <- consumption_family_servicepoint_meter_supply_MAINBYPASSSUB[meter_type!="SUB"]

consumption_family_servicepoint_meter_supply_SUB <- consumption_family_servicepoint_meter_supply_MAINBYPASSSUB[meter_type=="SUB"] %>%
  dplyr::group_by(service_point_sn) %>%
  dplyr::filter(move_in_date==max(move_in_date)) %>%
  as.data.table()
consumption_family_servicepoint_meter_supply <- rbind(consumption_family_servicepoint_meter_supply_MAINBYPASS,
                                                      consumption_family_servicepoint_meter_supply_SUB)

Consumption <- consumption_family_servicepoint_meter_supply   %>%
               dplyr::mutate(date=date(date_consumption),month.end=as.character(timeLastDayInMonth(date))) %>%
               dplyr::filter(!(site %in% c("Whampoa","Tuas")) & !(is.na(interpolated_consumption))) %>%
               dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","floor",
                               "date","month.end","adjusted_consumption")

MonthlyConsumption <- Consumption %>%
  dplyr::group_by(site,block,service_point_sn,pub_cust_id,meter_sn,meter_type,supply,floor,month.end) %>%
  dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption)) %>%
  dplyr::filter(month.end!=thismonth)

IndirectMasterMeters <- DirectMasterMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()

Indirect_MasterMeters <- Direct_MasterMeters <-list()
Indirect_SubMeters <- Direct_SubMeters <- list()

## to derive NetConsumption from MonthlyConsumption for each block
all_blocks <- sort(unique(MonthlyConsumption$block))
blocks <- all_blocks[1:length(all_blocks)-1]
last_block <- all_blocks[length(all_blocks)]

MonthlyConsumption_Master <- MonthlyConsumption %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS"))
MonthlyConsumption_SUB <- MonthlyConsumption %>%
  dplyr::filter(meter_type=="SUB")

for(j in 1:length(blocks)) {
  IndirectMasterMeters[[j]] <- MonthlyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  Indirect_MasterMeters[[j]] <- MonthlyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,month.end) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  DirectMasterMeters[[j]] <- MonthlyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  Direct_MasterMeters[[j]] <- MonthlyConsumption_Master  %>%
    dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,month.end) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  IndirectSubMeters[[j]] <- MonthlyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  Indirect_SubMeters[[j]] <- MonthlyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,month.end) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  DirectSubMeters[[j]] <- MonthlyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
  
  Direct_SubMeters[[j]] <- MonthlyConsumption_SUB  %>%
    dplyr::filter(block==blocks[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,month.end) %>%
    dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))
}

# for PG_B5, last_block
IndirectMasterMeters[[5]] <- MonthlyConsumption_Master  %>%
  dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

Indirect_MasterMeters[[5]] <- MonthlyConsumption_Master  %>%
  dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,month.end) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

DirectMasterMeters[[5]] <- MonthlyConsumption_Master  %>%
  dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

Direct_MasterMeters[[5]] <- MonthlyConsumption_Master  %>%
  dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,month.end) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

IndirectSubMeters[[5]] <- MonthlyConsumption_SUB  %>%
  dplyr::filter(block==last_block & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

Indirect_SubMeters[[5]] <- MonthlyConsumption_SUB  %>%
  dplyr::filter(block==last_block & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,month.end) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

DirectSubMeters[[5]] <- MonthlyConsumption_SUB  %>%
  dplyr::filter(block==last_block & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

Direct_SubMeters[[5]] <- MonthlyConsumption_SUB  %>%
  dplyr::filter(block==last_block & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,month.end) %>%
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption,na.rm=TRUE))

IndirectMasterMeters_DF <- rbindlist(IndirectMasterMeters) 
DirectMasterMeters_DF <- rbindlist(DirectMasterMeters) 
IndirectSubMeters_DF <- rbindlist(IndirectSubMeters) %>% dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>% 
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption)) %>% as.data.frame()
DirectSubMeters_DF <- rbindlist(DirectSubMeters) %>% dplyr::group_by(block,month.end,supply,meter_type,meter_sn) %>% 
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption)) %>% as.data.frame()
MasterMeters_DF <- rbind(IndirectMasterMeters_DF,DirectMasterMeters_DF)
SubMeters_DF <- rbind(IndirectSubMeters_DF,DirectSubMeters_DF)

MasterSubMeters_DF <- rbind(MasterMeters_DF,
                            SubMeters_DF[,c("block","month.end","supply","meter_type","meter_sn","MonthlyConsumption")]) 

## this is to get the NetConsumption and NetConsumptionRate
IndirectMaster_Meters <- rbindlist(Indirect_MasterMeters) %>% dplyr::rename(IndirectMaster=MonthlyConsumption)
DirectMaster_Meters <- rbindlist(Direct_MasterMeters) %>% dplyr::rename(DirectMaster=MonthlyConsumption)
IndirectSub_Meters <- rbindlist(Indirect_SubMeters) %>% dplyr::group_by(block,month.end) %>% 
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption)) %>%
  dplyr::rename(IndirectSub=MonthlyConsumption) %>% as.data.frame()
DirectSub_Meters <- rbindlist(Direct_SubMeters) %>% dplyr::group_by(block,month.end) %>% 
  dplyr::summarise(MonthlyConsumption=sum(MonthlyConsumption)) %>%
  dplyr::rename(DirectSub=MonthlyConsumption) %>% as.data.frame()
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
  dplyr::select_("block","month.end","IndirectMaster","IndirectSub","IndirectNetConsumption","IndirectNetConsumptionRate",
                 "DirectMaster","DirectSub","DirectNetConsumption","DirectNetConsumptionRate",
                 "TotalMaster","TotalNetConsumption","TotalNetConsumptionRate")

## used WeeklyNetConsumptionRate_xts to see which month and block has the NA
load("/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData")
Month_Block_NA <- WeeklyNetConsumption_NA[c(which(WeeklyNetConsumption_NA$TotalNetConsumptionRate=="NA")),1:2]

MonthlyNetConsumptionCSV <- MasterSub_Meters %>%
                    dplyr::mutate(IndirectSub=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,IndirectSub)) %>%
                    dplyr::mutate(IndirectNetConsumption=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,IndirectNetConsumption)) %>%
                    dplyr::mutate(IndirectNetConsumptionRate=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,IndirectNetConsumptionRate)) %>%
                    dplyr::mutate(DirectSub=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,DirectSub)) %>%
                    dplyr::mutate(DirectNetConsumption=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,DirectNetConsumption)) %>%
                    dplyr::mutate(DirectNetConsumptionRate=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,DirectNetConsumptionRate)) %>%
                    dplyr::mutate(TotalNetConsumption=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,TotalNetConsumption)) %>%
                    dplyr::mutate(TotalNetConsumptionRate=ifelse((block %in% Month_Block_NA$block &
                                                      month(month.end) %in% month(Month_Block_NA$LastDayofWeek) &
                                                      year(month.end) %in% year(Month_Block_NA$LastDayofWeek)),
                                                      NA,TotalNetConsumptionRate))

MonthlyNetConsumptionDate <- MonthlyNetConsumptionCSV %>% dplyr::select_("month.end") %>% unique()

MonthlyNetConsumptionCSV_TotalNetConsumption <- MonthlyNetConsumptionCSV %>% dplyr::select_("month.end","block","TotalNetConsumption")
MonthlyNetConsumptionCSV_TotalNetConsumption_wide <- spread(MonthlyNetConsumptionCSV_TotalNetConsumption,block,TotalNetConsumption)
MonthlyNetConsumptionCSV_TotalNetConsumption_wide["month.end"] <- NULL
MonthlyNetConsumption_xts <- xts(MonthlyNetConsumptionCSV_TotalNetConsumption_wide,order.by=as.Date(MonthlyNetConsumptionDate$month.end))

MonthlyNetConsumptionCSV_TotalNetConsumptionRate <- MonthlyNetConsumptionCSV %>% dplyr::select_("month.end","block","TotalNetConsumptionRate")
MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide <- spread(MonthlyNetConsumptionCSV_TotalNetConsumptionRate,block,TotalNetConsumptionRate)
MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide["month.end"] <- NULL
MonthlyNetConsumptionRate_xts <- xts(MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide,order.by=as.Date(MonthlyNetConsumptionDate$month.end))

MonthlyNetConsumption <- MonthlyNetConsumptionCSV %>% dplyr::select_("month.end","block",
                                                                     "IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")
MonthlyNetConsumption <- MonthlyNetConsumption %>% dplyr::arrange(desc(month.end))

MonthlyNetConsumption_NA <- MonthlyNetConsumption %>% 
  dplyr::mutate(IndirectNetConsumptionRate=ifelse(is.na(IndirectNetConsumptionRate),"NA",IndirectNetConsumptionRate),
                DirectNetConsumptionRate=ifelse(is.na(DirectNetConsumptionRate),"NA",DirectNetConsumptionRate),
                TotalNetConsumptionRate=ifelse(is.na(TotalNetConsumptionRate),"NA",TotalNetConsumptionRate))

MonthlyNetConsumptionCSV <- MonthlyNetConsumptionCSV %>% dplyr::arrange(desc(month.end))

write.csv(MonthlyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/MonthlyNetConsumption.csv",row.names=FALSE)

assign_NA <- MonthlyNetConsumptionCSV[which(is.na(MonthlyNetConsumptionCSV$TotalNetConsumptionRate)),1:2]
MonthlyPunggolNetConsumptionDetails <- MonthlyConsumption %>% dplyr::group_by(block,month.end,supply) %>%
  dplyr::mutate(NetConsumption=sum(MonthlyConsumption[meter_type!="SUB"])-sum(MonthlyConsumption[meter_type=="SUB"]))
MonthlyPunggolNetConsumptionDetails_NA <- MonthlyPunggolNetConsumptionDetails %>%
  dplyr::mutate(NetConsumption=ifelse((block %in% assign_NA$block & month.end %in% assign_NA$month.end),NA,
                                      NetConsumption)) %>% as.data.frame()
MonthlyPunggolNetConsumptionDetails_NA <- MonthlyPunggolNetConsumptionDetails_NA %>%
  dplyr::mutate(NetConsumption=ifelse(is.na(NetConsumption),"NA",NetConsumption))

write.csv(MonthlyPunggolNetConsumptionDetails_NA,"/srv/shiny-server/DataAnalyticsPortal/data/MonthlyPunggolNetConsumptionDetailsCSV.csv",row.names=FALSE)

Updated_DateTime_NetConsumptionDetails_Monthly <- paste("Last Updated on ",now(),"."," Next Update on ",now()+months(1),".",sep="")

save(MonthlyNetConsumption_NA,MonthlyNetConsumption_xts,MonthlyNetConsumptionRate_xts,
     MonthlyPunggolNetConsumptionDetails_NA,
     Updated_DateTime_NetConsumptionDetails_Monthly,
     file="/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/MonthlyNetConsumption.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumptionDetails_Monthly successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)