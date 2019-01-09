rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,stringr,ISOweek,fst)

date_past2months <- today()-60
begin_date <- date_past2months - mday(date_past2months) + 1

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last90days_servicepoint.fst",as.data.table = TRUE)
consumption_download <- consumption[date(date_consumption)>=begin_date] 

index <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/index_last90days_servicepoint.fst",as.data.table = TRUE)
index_download <- index[date(current_index_date)>=begin_date] 

family <- as.data.frame(tbl(con,"family")) %>% filter(address!="FAKE FAMILY" & status=="ACTIVE" & id_service_point!='601')
## exclude AHL
servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B" & service_point_sn != "3100660792")
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))
meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE" & id_real_estate!="3100660792") # exclude AHL
family_servicepoint_meter <- inner_join(family_servicepoint,meter,by=c("service_point_sn"="id_real_estate","meter_type"))
## total of 1178 which includes all MAIN, BYPASS,SUB meters from Punggol, Yuhua and Tuas, exclude AHL

all_blocks_PG <- sort(unique(consumption_download[site=="Punggol"]$block))
blocks_PG <- all_blocks_PG[1:length(all_blocks_PG)-1]
last_block_PG <- all_blocks_PG[length(all_blocks_PG)]

all_blocks_YH <- sort(unique(consumption_download[site=="Yuhua"]$block))

family_servicepoint_meter_supply_PG <- family_servicepoint_meter %>% dplyr::filter(site=="Punggol") %>%
                                       dplyr::mutate(supply=ifelse(block %in% blocks_PG & meter_type=="SUB" & floor <="#04","Direct",
                                                            ifelse(block %in% blocks_PG & meter_type=="SUB" & floor >="#05","Indirect",
                                                            ifelse(block %in% last_block_PG & meter_type=="SUB" & floor <="#05","Direct",
                                                            ifelse(block %in% last_block_PG & meter_type=="SUB" & floor >="#06","Indirect",
                                                            ifelse(meter_type!="SUB" & substr(meter_sn,1,2) %in% c("DP","DM","DN","SQ"),"Direct",
                                                            ifelse(meter_type!="SUB" & substr(meter_sn,1,2) %in% c("EP","SM","SL"),"Indirect",0)))))))
family_servicepoint_meter_supply_PG <- family_servicepoint_meter_supply_PG %>%
                                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","move_in_date","move_out_date")

## Yuhua's Direct and Indirect
## YH_B3 and YH_B7 has ONLY Direct Supply
## YH_B1: Direct: #01, #02, #03. Indirect: #04 to #25
## YH_B2: Direct: #01, #02, #03, #04, Indirect: #01-421, #05 to #10
## YH_B4: Direct: #01, #02, #03, Indirect: #04 to #12
## YH_B5: Direct: #01, #02, #03, #04(763,765,767), Indirect: #04 to #12
## YH_B6: Direct: #01, #02, #03, #04, #05, Indirect: #01--K2, #06 to #12

family_servicepoint_meter_supply_YH <- family_servicepoint_meter %>% dplyr::filter(site=="Yuhua") %>%
  dplyr::mutate(supply=ifelse(block %in% all_blocks_YH[c(3,7)],"Direct",
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
                       ifelse(block == all_blocks_YH[6] & service_point_sn=="3003308465","Indirect",0))))))))))))))
family_servicepoint_meter_supply_YH <- family_servicepoint_meter_supply_YH %>%
                                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","move_in_date","move_out_date")

family_servicepoint_meter_supply_Tuas <- family_servicepoint_meter %>% dplyr::filter(site=="Tuas") %>%
                                         dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type")
family_servicepoint_meter_supply_Tuas$supply <- 0
family_servicepoint_meter_supply_Tuas$move_in_date <- as.POSIXct(0,origin = "1900-01-01")
family_servicepoint_meter_supply_Tuas$move_out_date <- as.POSIXct(0,origin = "1900-01-01")

family_servicepoint_meter_supply_All <- rbind(rbind(family_servicepoint_meter_supply_PG,family_servicepoint_meter_supply_YH),family_servicepoint_meter_supply_Tuas)

consumption_download_family_servicepoint_meter_supply_MAINBYPASS <- inner_join(consumption_download,family_servicepoint_meter_supply_All,
                                                                               by=c("service_point_sn","site","block","meter_type")) %>%
                                                                     dplyr::filter(meter_type!="SUB")

consumption_download_family_servicepoint_meter_supply_SUB <- inner_join(consumption_download,family_servicepoint_meter_supply_All,
                                                                        by=c("service_point_sn","site","block","meter_type")) %>%
                                                             dplyr::group_by(service_point_sn) %>%
                                                             dplyr::filter(meter_type=="SUB" & move_in_date==max(move_in_date)) %>%
                                                             as.data.frame()
consumption_download_family_servicepoint_meter_supply <- rbind(consumption_download_family_servicepoint_meter_supply_MAINBYPASS,
                                                               consumption_download_family_servicepoint_meter_supply_SUB)

index_download_family_servicepoint_meter_supply_MAINBYPASS <- inner_join(index_download,family_servicepoint_meter_supply_All,
                                                                         by=c("service_point_sn","site","block","meter_type")) %>%
                                                                 dplyr::filter(meter_type!="SUB")

index_download_family_servicepoint_meter_supply_SUB <- inner_join(index_download,family_servicepoint_meter_supply_All,
                                                                  by=c("service_point_sn","site","block","meter_type")) %>%
                                                          dplyr::group_by(service_point_sn) %>%
                                                          dplyr::filter(meter_type=="SUB" & move_in_date==max(move_in_date)) %>%
                                                          as.data.frame()
index_download_family_servicepoint_meter_supply <- rbind(index_download_family_servicepoint_meter_supply_MAINBYPASS,
                                                         index_download_family_servicepoint_meter_supply_SUB)

ConsumptionDownload <- consumption_download_family_servicepoint_meter_supply   %>%
                       dplyr::mutate(date=date(date_consumption),time=substr(date_consumption,12,19)) %>%
                       dplyr::filter(!(is.na(interpolated_consumption))) %>%
                       dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","date","time","adjusted_consumption","index_value")

IndexDownload <- index_download_family_servicepoint_meter_supply  %>%
                 dplyr::mutate(date=date(current_index_date),time=substr(current_index_date,12,19)) %>%
                 dplyr::select_("site","block","service_point_sn","pub_cust_id","meter_sn","meter_type","supply","date","time","index")

ConsumptionDownload$week <- gsub("-W","_",str_sub(date2ISOweek(ConsumptionDownload$date),end = -3)) # convert date to week
IndexDownload$week <- gsub("-W","_",str_sub(date2ISOweek(IndexDownload$date),end = -3)) # convert date to week

names(ConsumptionDownload)[names(ConsumptionDownload) == 'adjusted_consumption'] <- 'consumption'
names(ConsumptionDownload)[names(ConsumptionDownload) == 'index_value'] <- 'index'

Updated_DateTime_ConsumptionIndexDownload <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

write.fst(ConsumptionDownload,"/srv/shiny-server/DataAnalyticsPortal/data/DT/ConsumptionDownload.fst",100)
write.fst(IndexDownload,"/srv/shiny-server/DataAnalyticsPortal/data/DT/IndexDownload.fst",100)

save(begin_date,Updated_DateTime_ConsumptionIndexDownload,file="/srv/shiny-server/DataAnalyticsPortal/data/Updated_DateTime_ConsumptionIndexDownload.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionIndexDownload_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)