rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,RPostgreSQL,stringr,ISOweek,lubridate,data.table,tidyr,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today_week <- gsub("-W","_",str_sub(date2ISOweek(today()),end = -3)) # convert date to week

consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",as.data.table = TRUE)

Punggol_SUB <- consumption_last6months_servicepoint[site=="Punggol" & meter_type=="SUB" & adjusted_consumption != "NA"] # contain data on sub meter, including childcare
Yuhua_SUB <- consumption_last6months_servicepoint[site=="Yuhua" & meter_type=="SUB" & adjusted_consumption != "NA"] 

all_blocks_PG <- sort(unique(Punggol_SUB$block))  
blocks_PG <- all_blocks_PG[1:length(all_blocks_PG)-1]
last_block_PG <- all_blocks_PG[length(all_blocks_PG)]

all_blocks_YH <- sort(unique(Yuhua_SUB$block))  

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

family_servicepoint_meter_supply_YH <- family_servicepoint_meter[site=="Yuhua"]

family_servicepoint_meter_supply_YH <- family_servicepoint_meter_supply_YH[,supply:=ifelse(block %in% all_blocks_YH[c(3,7)],"Direct",
                                                                                    ifelse(block == all_blocks_YH[1] & floor %in% c("#01","#02","#03"),"Direct",
                                                                                    ifelse(block == all_blocks_YH[1] & !floor %in% c("#01","#02","#03"),"Indirect",
                                                                                    ifelse(block == all_blocks_YH[2] & floor %in% c("#01","#02","#03","#04") & service_point_sn!="3003371973","Direct",
                                                                                    ifelse(block == all_blocks_YH[2] & !floor %in% c("#01","#02","#03","#04"),"Indirect",
                                                                                    ifelse(block == all_blocks_YH[2] & service_point_sn=="3003371973","Indirect",
                                                                                    ifelse(block == all_blocks_YH[4] & floor %in% c("#01","#02","#03"),"Direct",
                                                                                    ifelse(block == all_blocks_YH[4] & !floor %in% c("#01","#02","#03"),"Indirect",
                                                                                    ifelse(block == all_blocks_YH[5] & floor %in% c("#01","#02","#03") | service_point_sn %in% c("3001106956","3001108108","3001108786"),"Direct",
                                                                                    ifelse(block == all_blocks_YH[5] & !floor %in% c("#01","#02","#03"),"Indirect",
                                                                                    ifelse(block == all_blocks_YH[6] & floor %in% c("#01","#02","#03","#04","#05") & service_point_sn!="3003308465","Direct",
                                                                                    ifelse(block == all_blocks_YH[6] & !floor %in% c("#01","#02","#03","#04","#05"),"Indirect",
                                                                                    ifelse(block == all_blocks_YH[6] & service_point_sn=="3003308465","Indirect",0)))))))))))))]

family_servicepoint_meter_supply_YH <- family_servicepoint_meter_supply_YH[,c("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","move_in_date","move_out_date")]

family_servicepoint_meter_supply_PGYH <- rbind(family_servicepoint_meter_supply_PG,family_servicepoint_meter_supply_YH)

setkey(consumption_last6months_servicepoint,service_point_sn,meter_type,block,site)
setkey(family_servicepoint_meter_supply_PGYH,service_point_sn,meter_type,block,site)

consumption_family_servicepoint_meter_supply_MAINBYPASSSUB <- consumption_last6months_servicepoint[family_servicepoint_meter_supply_PGYH,nomatch=0]

consumption_family_servicepoint_meter_supply_MAINBYPASS <- consumption_family_servicepoint_meter_supply_MAINBYPASSSUB[meter_type!="SUB"]

consumption_family_servicepoint_meter_supply_SUB <- consumption_family_servicepoint_meter_supply_MAINBYPASSSUB[meter_type=="SUB"] %>%
                                                    dplyr::group_by(service_point_sn) %>%
                                                    dplyr::filter(move_in_date==max(move_in_date)) %>%
                                                    as.data.table()
consumption_family_servicepoint_meter_supply <- rbind(consumption_family_servicepoint_meter_supply_MAINBYPASS,
                                                      consumption_family_servicepoint_meter_supply_SUB)

consumption_family_servicepoint_meter_supply$week <- gsub("-W","_",str_sub(date2ISOweek(date(consumption_family_servicepoint_meter_supply$date_consumption)),end = -3)) 
# convert date to week

Consumption <- consumption_family_servicepoint_meter_supply  %>%
  dplyr::group_by(block,service_point_sn,meter_sn,meter_type,supply,week) %>%
  dplyr::mutate(Date=date(date_consumption),LastDayofWeek=max(Date)) %>% 
  dplyr::filter(!(is.na(adjusted_consumption))) %>%
  dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","floor",
                 "Date","week","LastDayofWeek","adjusted_consumption")

WeeklyConsumption <- Consumption %>%
  dplyr::group_by(site,block,service_point_sn,pub_cust_id,meter_sn,meter_type,supply,floor,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(adjusted_consumption)) %>%
  dplyr::filter(week!=today_week)

IndirectMasterMeters_PG <- DirectMasterMeters_PG <-list()
IndirectSubMeters_PG <- DirectSubMeters_PG <- list()

Indirect_MasterMeters_PG <- Direct_MasterMeters_PG <-list()
Indirect_SubMeters_PG <- Direct_SubMeters_PG <- list()

IndirectMasterMeters_YH <- DirectMasterMeters_YH <-list()
IndirectSubMeters_YH <- DirectSubMeters_YH <- list()

Indirect_MasterMeters_YH <- Direct_MasterMeters_YH <-list()
Indirect_SubMeters_YH <- Direct_SubMeters_YH <- list()

WeeklyConsumption_Master <- WeeklyConsumption %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS"))
WeeklyConsumption_SUB <- WeeklyConsumption %>%
  dplyr::filter(meter_type=="SUB")

for(j in 1:length(blocks_PG)) {
  IndirectMasterMeters_PG[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks_PG[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Indirect_MasterMeters_PG[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks_PG[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  DirectMasterMeters_PG[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks_PG[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Direct_MasterMeters_PG[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==blocks_PG[j] & !grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  IndirectSubMeters_PG[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks_PG[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Indirect_SubMeters_PG[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks_PG[j] & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  DirectSubMeters_PG[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks_PG[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Direct_SubMeters_PG[[j]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(block==blocks_PG[j] & (floor=="#02" | floor=="#03" | floor=="#04")) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
}

# for PG_B5, last_block
IndirectMasterMeters_PG[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block_PG & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_MasterMeters_PG[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block_PG & grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectMasterMeters_PG[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block_PG & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_MasterMeters_PG[[5]] <- WeeklyConsumption_Master  %>%
  dplyr::filter(block==last_block_PG & !grepl("M|B",service_point_sn)) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectSubMeters_PG[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block_PG & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters_PG[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block_PG & !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectSubMeters_PG[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block_PG & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_SubMeters_PG[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(block==last_block_PG & (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectMasterMeters_PG <- rbindlist(IndirectMasterMeters_PG) 
DirectMasterMeters_PG <- rbindlist(DirectMasterMeters_PG) 
IndirectSubMeters_PG <- rbindlist(IndirectSubMeters_PG) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
                        dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()
DirectSubMeters_PG <- rbindlist(DirectSubMeters_PG) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
                      dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()
MasterMeters_PG <- rbind(IndirectMasterMeters_PG,DirectMasterMeters_PG)
SubMeters_PG <- rbind(IndirectSubMeters_PG,DirectSubMeters_PG)

MasterSubMeters_PG <- rbind(MasterMeters_PG,
                            SubMeters_PG[,c("block","LastDayofWeek","supply","meter_type","meter_sn","WeeklyConsumption")]) 

## above is for Punggol Net Consumption. Below is for Yuhua Net Consumption
# YH_B3,YH_B7 has no MAIN/BYPASS meters
# 
# YH_B1,YH_B2,YH_B4,YH_B5,YH_B6: NetConsumption(Indirect)= Direct MAIN/BYPASS - Indirect SUB
# YH_B3,YH_B7: NetConsumption(Direct)= sum(Direct) SUB
YH_DirectBlocks <- all_blocks_YH[c(1,2,4,5,6)]
for(j in 1:length(YH_DirectBlocks)) {
  DirectMasterMeters_YH[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==YH_DirectBlocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,LastDayofWeek,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
  Direct_MasterMeters_YH[[j]] <- WeeklyConsumption_Master  %>%
    dplyr::filter(block==YH_DirectBlocks[j] & grepl("M|B",service_point_sn)) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
}

IndirectSubMeters_YH[[1]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B1" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
    dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
Indirect_SubMeters_YH[[1]] <- WeeklyConsumption_SUB  %>%
    dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B1" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
    dplyr::group_by(block,week,LastDayofWeek) %>%
    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectSubMeters_YH[[2]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B2" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters_YH[[2]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B2" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))
  
IndirectSubMeters_YH[[4]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B4" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters_YH[[4]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B4" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectSubMeters_YH[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B5" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters_YH[[5]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B5" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

IndirectSubMeters_YH[[6]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B6" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Indirect_SubMeters_YH[[6]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B6" & supply=="Indirect" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectSubMeters_YH[[3]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B3" & supply=="Direct" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_SubMeters_YH[[3]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B3" & supply=="Direct" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

DirectSubMeters_YH[[7]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B7" & supply=="Direct" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

Direct_SubMeters_YH[[7]] <- WeeklyConsumption_SUB  %>%
  dplyr::filter(service_point_sn %in% family_servicepoint_meter_supply_YH[block=="YH_B7" & supply=="Direct" & meter_type=="SUB"]$service_point_sn) %>%
  dplyr::group_by(block,week,LastDayofWeek) %>%
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption,na.rm=TRUE))

## Yuhua
DirectMasterMeters_YH <- rbindlist(DirectMasterMeters_YH) 
IndirectSubMeters_YH <- rbindlist(IndirectSubMeters_YH) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()
DirectSubMeters_YH <- rbindlist(DirectSubMeters_YH) %>% dplyr::group_by(block,LastDayofWeek,week,supply,meter_type,meter_sn) %>% 
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>% as.data.frame()

MasterMeters_YH <- DirectMasterMeters_YH
SubMeters_YH <- rbind(IndirectSubMeters_YH,DirectSubMeters_YH)
MasterSubMeters_YH <- rbind(MasterMeters_YH,
                            SubMeters_YH[,c("block","LastDayofWeek","supply","meter_type","meter_sn","WeeklyConsumption")]) 

## this is to get the NetConsumption and NetConsumptionRate
IndirectMaster_Meters_PG <- rbindlist(Indirect_MasterMeters_PG) %>% dplyr::rename(IndirectMaster=WeeklyConsumption)
DirectMaster_Meters_PG <- rbindlist(Direct_MasterMeters_PG) %>% dplyr::rename(DirectMaster=WeeklyConsumption)
IndirectSub_Meters_PG <- rbindlist(Indirect_SubMeters_PG) %>% dplyr::group_by(block,week) %>% 
                      dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>%
                      dplyr::rename(IndirectSub=WeeklyConsumption) %>% as.data.frame()
DirectSub_Meters_PG <- rbindlist(Direct_SubMeters_PG) %>% dplyr::group_by(block,week) %>% 
                    dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>%
                    dplyr::rename(DirectSub=WeeklyConsumption) %>% as.data.frame()
Master_Meters_PG <- cbind(IndirectMaster_Meters_PG,DirectMaster_Meters_PG[,c("DirectMaster")])
Sub_Meters_PG <- cbind(IndirectSub_Meters_PG,DirectSub_Meters_PG[,c("DirectSub")])
colnames(Sub_Meters_PG)[4] <- "DirectSub"
MasterSub_Meters_PG <- cbind(Master_Meters_PG,Sub_Meters_PG[,c("IndirectSub","DirectSub")]) %>%
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

SubMetersCountPerWeekPerBlock_PG <- SubMeters_PG %>% dplyr::group_by(block,week) %>%
                                    dplyr::summarise(MeterCount=n())

MasterSubMeters_Counts_PG <- cbind(MasterSub_Meters_PG,SubMetersCountPerWeekPerBlock_PG[3])

## to find number of meters per block
MetersPerBlock_PG <- SubMeters_PG %>% dplyr::group_by(block) %>%
                  dplyr::summarise(TotalMeters=length(unique(meter_sn)))
SUB_CountsPerBlock_PG <- rep(c(MetersPerBlock_PG$TotalMeters),each=nrow(MasterSub_Meters_PG)/length(all_blocks_PG)) %>% as.data.frame()
colnames(SUB_CountsPerBlock_PG) <- "SupposedCount"

MasterSubMetersCounts_PG <- cbind(MasterSubMeters_Counts_PG,SUB_CountsPerBlock_PG) %>%
                              dplyr::mutate(DiffCount=MeterCount-SupposedCount) %>%
                              dplyr::mutate(IndirectSub=ifelse(DiffCount!=0,NA,IndirectSub)) %>%
                              dplyr::mutate(IndirectNetConsumption=ifelse(DiffCount!=0,NA,IndirectNetConsumption)) %>%
                              dplyr::mutate(IndirectNetConsumptionRate=ifelse(DiffCount!=0,NA,IndirectNetConsumptionRate)) %>%
                              dplyr::mutate(DirectSub=ifelse(DiffCount!=0,NA,DirectSub)) %>%
                              dplyr::mutate(DirectNetConsumption=ifelse(DiffCount!=0,NA,DirectNetConsumption)) %>%
                              dplyr::mutate(DirectNetConsumptionRate=ifelse(DiffCount!=0,NA,DirectNetConsumptionRate)) %>%
                              dplyr::mutate(TotalNetConsumption=ifelse(DiffCount!=0,NA,TotalNetConsumption)) %>%
                              dplyr::mutate(TotalNetConsumptionRate=ifelse(DiffCount!=0,NA,TotalNetConsumptionRate)) 

## Yuhua
# YH_B1,YH_B2,YH_B4,YH_B5,YH_B6: NetConsumption(Indirect)= Direct MAIN/BYPASS - Indirect SUB
# YH_B3,YH_B7: NetConsumption(Direct)= sum(Direct) SUB
DirectMaster_Meters_YH <- rbindlist(Direct_MasterMeters_YH) %>% dplyr::rename(DirectMaster=WeeklyConsumption)
IndirectSub_Meters_YH <- rbindlist(Indirect_SubMeters_YH) %>% dplyr::group_by(block,week) %>% 
  dplyr::summarise(WeeklyConsumption=sum(WeeklyConsumption)) %>%
  dplyr::rename(IndirectSub=WeeklyConsumption) %>% as.data.frame()
NetConsumption_Indirect_YH <- inner_join(DirectMaster_Meters_YH,IndirectSub_Meters_YH,by=c("block","week")) %>%
                              dplyr::mutate(IndirectNetConsumption=DirectMaster-IndirectSub)
DirectSub_Meters_YH <- rbindlist(Direct_SubMeters_YH)
NetConsumption_Direct_YH <- DirectSub_Meters_YH 
colnames(NetConsumption_Direct_YH)[3] <- "DirectNetConsumption"
## Yuhua

WeeklyNetConsumptionCSV <- MasterSubMetersCounts_PG[,c(1:13)]

WeeklyNetConsumptionDate <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek") %>% unique()

WeeklyNetConsumptionCSV_TotalNetConsumption <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek","block","TotalNetConsumption")
WeeklyNetConsumptionCSV_TotalNetConsumption_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumption,block,TotalNetConsumption)
WeeklyNetConsumptionCSV_TotalNetConsumption_wide["LastDayofWeek"] <- NULL
WeeklyNetConsumption_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumption_wide,order.by=as.Date(WeeklyNetConsumptionDate$LastDayofWeek))

WeeklyNetConsumptionCSV_TotalNetConsumptionRate <- WeeklyNetConsumptionCSV %>% dplyr::select_("LastDayofWeek","block","TotalNetConsumptionRate")
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumptionRate,block,TotalNetConsumptionRate)
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide["LastDayofWeek"] <- NULL
WeeklyNetConsumptionRate_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide,order.by=as.Date(WeeklyNetConsumptionDate$LastDayofWeek))

WeeklyNetConsumption <- MasterSubMetersCounts_PG %>% dplyr::select_("LastDayofWeek","block",
                                                                  "IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")
WeeklyNetConsumption <- WeeklyNetConsumption %>% dplyr::arrange(desc(LastDayofWeek))
WeeklyNetConsumption_NA <- WeeklyNetConsumption %>% 
                           dplyr::mutate(IndirectNetConsumptionRate=ifelse(is.na(IndirectNetConsumptionRate),"NA",IndirectNetConsumptionRate),
                                         DirectNetConsumptionRate=ifelse(is.na(DirectNetConsumptionRate),"NA",DirectNetConsumptionRate),
                                         TotalNetConsumptionRate=ifelse(is.na(TotalNetConsumptionRate),"NA",TotalNetConsumptionRate))

WeeklyNetConsumptionCSV <- WeeklyNetConsumptionCSV %>% dplyr::arrange(desc(LastDayofWeek))

write.csv(WeeklyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.csv",row.names=FALSE)

assign_NA <- WeeklyNetConsumptionCSV[which(is.na(WeeklyNetConsumptionCSV$TotalNetConsumptionRate)),1:2]
WeeklyPunggolNetConsumptionDetails <- WeeklyConsumption %>% filter(site=="Punggol") %>% dplyr::group_by(block,week,supply) %>%
                                      dplyr::mutate(NetConsumption=sum(WeeklyConsumption[meter_type!="SUB"])-sum(WeeklyConsumption[meter_type=="SUB"]))

WeeklyPunggolNetConsumptionDetails_NA_list <- list()
for (j in 1:nrow(assign_NA)){
WeeklyPunggolNetConsumptionDetails_NA_list[[j]] <- WeeklyPunggolNetConsumptionDetails %>%
  dplyr::mutate(NetConsumption=ifelse((block == assign_NA[j,1] & LastDayofWeek == assign_NA[j,2]),NA,
                                      NetConsumption)) %>% as.data.frame() %>%
  dplyr::filter(block == assign_NA[j,1] & LastDayofWeek == assign_NA[j,2] & is.na(NetConsumption))
}

WeeklyPunggolNetConsumptionDetails_NA <- rbindlist(WeeklyPunggolNetConsumptionDetails_NA_list)
WeeklyPunggolNetConsumptionDetails_NA <- WeeklyPunggolNetConsumptionDetails_NA %>% unique()
WeeklyPunggolNetConsumptionDetails_NA <- WeeklyPunggolNetConsumptionDetails_NA %>%
                                         dplyr::mutate(NetConsumption=ifelse(is.na(NetConsumption),"NA",NetConsumption))

WeeklyPunggolNetConsumptionDetails_nonNA_list <- list()
for (j in 1:nrow(assign_NA)){
  WeeklyPunggolNetConsumptionDetails_nonNA_list[[j]] <- WeeklyPunggolNetConsumptionDetails %>%
    dplyr::filter(block == assign_NA[j,1] & LastDayofWeek == assign_NA[j,2] & !is.na(NetConsumption))
}
WeeklyPunggolNetConsumptionDetails_nonNA <- rbindlist(WeeklyPunggolNetConsumptionDetails_nonNA_list)

## remove WeeklyPunggolNetConsumptionDetails_nonNA from WeeklyPunggolNetConsumptionDetails, and add
## WeeklyPunggolNetConsumptionDetails_NA

# difference btw 2 dataframe/data.table
setdiffDF <- function(A, B){
  f <- function(A, B)
    A[!duplicated(rbind(B, A))[nrow(B) + 1:nrow(A)], ]
  df1 <- f(A, B)
  df2 <- f(B, A)
  rbind(df1, df2)
}

WeeklyPunggolNetConsumptionDetails_Diff <- setdiffDF(as.data.frame(WeeklyPunggolNetConsumptionDetails), WeeklyPunggolNetConsumptionDetails_nonNA)

WeeklyPunggolNetConsumptionDetails_NA <- rbind(WeeklyPunggolNetConsumptionDetails_Diff,WeeklyPunggolNetConsumptionDetails_NA)

write.csv(WeeklyPunggolNetConsumptionDetails_NA,"/srv/shiny-server/DataAnalyticsPortal/data/WeeklyPunggolNetConsumptionDetailsCSV.csv",row.names=FALSE)

Updated_DateTime_NetConsumptionDetails_Weekly <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(WeeklyNetConsumption_NA,WeeklyNetConsumption_xts,WeeklyNetConsumptionRate_xts,
     WeeklyPunggolNetConsumptionDetails_NA,
     Updated_DateTime_NetConsumptionDetails_Weekly,
     file="/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/WeeklyNetConsumption.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumptionDetails_Weekly successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)