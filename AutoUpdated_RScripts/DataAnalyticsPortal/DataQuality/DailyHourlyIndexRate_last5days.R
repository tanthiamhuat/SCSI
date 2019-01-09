rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

last5days <- today()-5
today <- today()
index_last5days <- as.data.frame(tbl(con,"index") %>% dplyr::filter(date(current_index_date) >= last5days &
                                                              date(current_index_date) < today))

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% dplyr::filter(site=="Punggol")
servicepoint_available_Tuas <- servicepoint_available %>% dplyr::filter(site=="Tuas")
servicepoint_available_Yuhua <- servicepoint_available %>% dplyr::filter(site=="Yuhua")

servicepoint_available_Punggol_Blocks <- servicepoint_available_Punggol %>% 
                                         dplyr::group_by(block) %>%
                                         dplyr::summarise(MeterCount=n())

servicepoint_available_Yuhua_Blocks <- servicepoint_available_Yuhua %>% 
                                       dplyr::group_by(block) %>%
                                       dplyr::summarise(MeterCount=n())

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index_servicepoint_meter <- inner_join(servicepoint_meter,index_last5days,by=c("id.x"="id_service_point"))

index_data_hourly <- index_servicepoint_meter %>%
                     select(meter_sn,current_index_date,block,site) %>%
                     mutate(date=substr(current_index_date,1,10),hour=hour(current_index_date)) %>%
                     dplyr::filter(meter_sn!="WJ003984A") # exclude AHL
index_data_hourly$date <- as.Date(index_data_hourly$date)

colnames(index_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","date","hour")

DailyIndexReadingRate <- index_data_hourly %>%
                         dplyr::group_by(date) %>%
                         dplyr::summarise(TotalMeters=sum(n_distinct(MeterSerialNumber))) %>%
                         dplyr::mutate(DailyIndexReadingRate=round(TotalMeters/(nrow(servicepoint_available_Punggol)+
                                                                                nrow(servicepoint_available_Tuas)+
                                                                                nrow(servicepoint_available_Yuhua))*100,2))

HourlyIndexReadingRate <- index_data_hourly %>% 
                          dplyr::group_by(date,hour) %>%
                          dplyr::summarise(CountPerHour=n())%>%
                          dplyr::group_by(date) %>%
                          dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                           HourlyIndexReadingRate=MeanCountPerHour/(nrow(servicepoint_available_Punggol)+
                                                                                    nrow(servicepoint_available_Tuas)+
                                                                                    nrow(servicepoint_available_Yuhua))*100)

HourlyIndexReadingRate_Punggol <- index_data_hourly %>% dplyr::filter(site=="Punggol") %>%
                                  dplyr::group_by(date,hour) %>%
                                  dplyr::summarise(CountPerHour=n())%>%
                                  dplyr::group_by(date) %>%
                                  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                   HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Punggol)*100)

HourlyIndexReadingRate_Tuas <- index_data_hourly %>% dplyr::filter(site=="Tuas") %>%
                               dplyr::group_by(date,hour) %>%
                               dplyr::summarise(CountPerHour=n())%>%
                               dplyr::group_by(date) %>%
                               dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Tuas)*100)

HourlyIndexReadingRate_Yuhua <- index_data_hourly %>% dplyr::filter(site=="Yuhua") %>%
                                dplyr::group_by(date,hour) %>%
                                dplyr::summarise(CountPerHour=n())%>%
                                dplyr::group_by(date) %>%
                                dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                 HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Yuhua)*100)

HourlyIndexReadingRate_Tuas_Customers <- index_data_hourly %>% dplyr::filter(site=="Tuas") %>%
  dplyr::group_by(MeterSerialNumber,date) %>%
  dplyr::summarise(CountPerDay=n())%>%
  dplyr::group_by(MeterSerialNumber) %>%
  dplyr::summarise(IndexReadingRate=(sum(CountPerDay)/(24*5))*100)

HourlyIndexReadingRate_Punggol_blocks <- index_data_hourly %>% dplyr::filter(site=="Punggol") %>%
                                         dplyr::group_by(date,hour,block) %>%
                                         dplyr::summarise(CountPerHour=n())%>%
                                         dplyr::group_by(date,block) %>%
                                         dplyr::summarise(MeanCountPerHour=mean(CountPerHour))

servicepoint_available_Punggol_Blocks_Loop <- servicepoint_available_Punggol_Blocks[rep(seq_len(nrow(servicepoint_available_Punggol_Blocks)), 
                                                                                        times=nrow(HourlyIndexReadingRate_Punggol_blocks)/nrow(servicepoint_available_Punggol_Blocks)),] 

HourlyIndexReadingRate_PG_Blocks <- cbind.data.frame(HourlyIndexReadingRate_Punggol_blocks[1:nrow(servicepoint_available_Punggol_Blocks_Loop),],
                                                 servicepoint_available_Punggol_Blocks_Loop) 
HourlyIndexReadingRate_PG_Blocks[4] <- NULL  
HourlyIndexReadingRate_PunggolBlocks <- HourlyIndexReadingRate_PG_Blocks %>%
                                        dplyr::mutate(HourlyIndexReadingRate=round(MeanCountPerHour/MeterCount*100,2))

HourlyIndexReadingRate_Yuhua_blocks <- index_data_hourly %>% dplyr::filter(site=="Yuhua") %>%
  dplyr::group_by(date,hour,block) %>%
  dplyr::summarise(CountPerHour=n())%>%
  dplyr::group_by(date,block) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour))

servicepoint_available_Yuhua_Blocks_Loop <- servicepoint_available_Yuhua_Blocks[rep(seq_len(nrow(servicepoint_available_Yuhua_Blocks)), 
                                                                                        times=nrow(HourlyIndexReadingRate_Yuhua_blocks)/nrow(servicepoint_available_Yuhua_Blocks)),] 

HourlyIndexReadingRate_YH_Blocks <- cbind.data.frame(HourlyIndexReadingRate_Yuhua_blocks[1:nrow(servicepoint_available_Yuhua_Blocks_Loop),],
                                                     servicepoint_available_Yuhua_Blocks_Loop) 
HourlyIndexReadingRate_YH_Blocks[4] <- NULL  
HourlyIndexReadingRate_YuhuaBlocks <- HourlyIndexReadingRate_YH_Blocks %>%
  dplyr::mutate(HourlyIndexReadingRate=round(MeanCountPerHour/MeterCount*100,2))

f <- list(
  family = "Courier New, monospace",
  size = 18,
  color = "#7f7f7f"
)

x <- list(
  title = "Dates",
  titlefont = f
)
y <- list(
  title = "Reading Rates (%)",
  titlefont = f
)

Updated_DateTime_DataQuality <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(f,x,y,DailyIndexReadingRate,HourlyIndexReadingRate,HourlyIndexReadingRate_Punggol,HourlyIndexReadingRate_Tuas,HourlyIndexReadingRate_Yuhua,
     HourlyIndexReadingRate_Tuas_Customers,HourlyIndexReadingRate_PunggolBlocks,HourlyIndexReadingRate_YuhuaBlocks,
     Updated_DateTime_DataQuality,
     file=paste0(path,'data/DailyHourlyIndexRate_last5days.RData'))

time_taken <- proc.time() - ptm
ans <- paste("DailyHourlyIndexRate_last5days successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)