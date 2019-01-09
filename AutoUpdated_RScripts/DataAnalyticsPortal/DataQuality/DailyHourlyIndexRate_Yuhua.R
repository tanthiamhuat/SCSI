rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

index_StartEnd <- as.data.frame(tbl(con,"index") %>% filter(date(current_index_date) >= "2018-04-01" & date(current_index_date) <= "2018-09-12"  
                                                            & id_service_point !=601))

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(!service_point_sn %in% c("3100507837M","3100507837B","3100660792"))
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="Yuhua")

servicepoint_available_Yuhua_Blocks <- servicepoint_available_Yuhua %>% 
                                       group_by(block) %>%
                                       dplyr::summarise(MeterCount=n())

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index_servicepoint_meter <- inner_join(servicepoint_meter,index_StartEnd,by=c("id.x"="id_service_point"))

index_data_hourly <- index_servicepoint_meter %>%
                     select(meter_sn,current_index_date,block,site) %>%
                     mutate(date=substr(current_index_date,1,10),hour=hour(current_index_date)) %>%
                     dplyr::filter(meter_sn!="WJ003984A") # exclude AHL
index_data_hourly$date <- as.Date(index_data_hourly$date)

colnames(index_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","date","hour")

HourlyIndexReadingRate_Yuhua <- index_data_hourly %>% filter(site=="Yuhua") %>%
                                group_by(date,hour) %>%
                                dplyr::summarise(CountPerHour=n())%>%
                                group_by(date) %>%
                                dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                 HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Yuhua)*100)

HourlyIndexReadingRate_Yuhua_blocks <- index_data_hourly %>% filter(site=="Yuhua") %>%
  group_by(date,hour,block) %>%
  dplyr::summarise(CountPerHour=n())%>%
  group_by(date,block) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour))

servicepoint_available_Yuhua_Blocks_Loop <- servicepoint_available_Yuhua_Blocks[rep(seq_len(nrow(servicepoint_available_Yuhua_Blocks)), 
                                                                                        times=nrow(HourlyIndexReadingRate_Yuhua_blocks)/nrow(servicepoint_available_Yuhua_Blocks)),] 

HourlyIndexReadingRate_YH_Blocks <- cbind.data.frame(HourlyIndexReadingRate_Yuhua_blocks[1:nrow(servicepoint_available_Yuhua_Blocks_Loop),],
                                                     servicepoint_available_Yuhua_Blocks_Loop) 
HourlyIndexReadingRate_YH_Blocks[4] <- NULL  
HourlyIndexReadingRate_YuhuaBlocks <- HourlyIndexReadingRate_YH_Blocks %>%
  dplyr::mutate(HourlyIndexReadingRate=round(MeanCountPerHour/MeterCount*100,2))

HourlyIndexReadingRate_YuhuaBlocks <- HourlyIndexReadingRate_YuhuaBlocks %>% dplyr::select("date","block","HourlyIndexReadingRate")


### Yuhua Blocks
blocks_YH <- sort(unique(HourlyIndexReadingRate_YuhuaBlocks$block))

for (i in 1:length(blocks_YH)){
  assign(paste("HourlyIndexReadingRate_",blocks_YH[i],sep=""), HourlyIndexReadingRate_YuhuaBlocks %>% filter(block==blocks_YH[i]))
}

HourlyIndexReadingRate_YuhuaBlocksDate <-HourlyIndexReadingRate_YuhuaBlocks %>% dplyr::select_("date") %>% unique()

HourlyIndexReadingRate_YuhuaBlockList <- list()
for(i in 1:length(blocks_YH)){
  HourlyIndexReadingRate_YuhuaBlockList[[i]] <- HourlyIndexReadingRate_YuhuaBlocks %>% filter(block==blocks_YH[i]) %>% 
    dplyr::select_("HourlyIndexReadingRate")
}

HourlyIndexRate_YuhuaBlocks <- as.data.frame(unlist(HourlyIndexReadingRate_YuhuaBlockList))
nr <- nrow(HourlyIndexReadingRate_YuhuaBlocks)
n <- nr/length(blocks_YH)
HourlyIndexRate_YuhuaBlocks <- as.data.frame(split(HourlyIndexRate_YuhuaBlocks, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(HourlyIndexRate_YuhuaBlocks) <- blocks_YH

HourlyIndexRate_YuhuaBlocks_xts <- xts(HourlyIndexRate_YuhuaBlocks,order.by=as.Date(HourlyIndexReadingRate_YuhuaBlocksDate$date))

HourlyIndexRatePerYuhuaBlock <- as.data.table(HourlyIndexRate_YuhuaBlocks_xts)
colnames(HourlyIndexRatePerYuhuaBlock)[1] <- "Date"

write.csv(HourlyIndexRatePerYuhuaBlock,file="/srv/shiny-server/DataAnalyticsPortal/data/HourlyIndexRatePerYuhuaBlock.csv")

graph <- dygraph(HourlyIndexRate_YuhuaBlocks_xts, main = "Hourly Index Rate Per Yuhua Block") %>%
  dyRangeSelector() %>%
  dyOptions(colors = c("#911eb4","#000080","#800000","#d2f53c","#fabebe","#008080","#000000"))
graph



