rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

last90days <- today()-90
today <- today()
index_last90days <- as.data.frame(tbl(con,"index") %>% filter(date(current_index_date) >= last90days &
                                                              date(current_index_date) < today))

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

servicepoint_available_Punggol_Blocks <- servicepoint_available_Punggol %>% 
                                         group_by(block) %>%
                                         dplyr::summarise(MeterCount=n())

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index_servicepoint_meter <- inner_join(servicepoint_meter,index_last90days,by=c("id.x"="id_service_point"))

index_data_hourly <- index_servicepoint_meter %>%
                     select(meter_sn,current_index_date,block,site) %>%
                     mutate(date=substr(current_index_date,1,10),hour=hour(current_index_date))
index_data_hourly$date <- as.Date(index_data_hourly$date)

colnames(index_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","date","hour")

index_data_daily <- index_data_hourly %>%
                    select(MeterSerialNumber,ReadingDate,block,site) %>%
                    dplyr::mutate(date=substr(ReadingDate,1,10),hour=hour(ReadingDate))

DailyIndexReadingRate <- index_data_daily %>% 
                         group_by(date) %>%
                         dplyr::summarise(TotalMeters=sum(n_distinct(MeterSerialNumber))) %>%
                         dplyr::mutate(DailyIndexReadingRate=round(TotalMeters/nrow(servicepoint_available)*100,2))
  
HourlyIndexReadingRate <- index_data_hourly %>% 
                          group_by(date,hour) %>%
                          dplyr::summarise(CountPerHour=n())%>%
                          group_by(date) %>%
                          dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                           HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available)*100)

HourlyIndexReadingRate_Punggol <- index_data_hourly %>% filter(site=="Punggol") %>%
                                  group_by(date,hour) %>%
                                  dplyr::summarise(CountPerHour=n())%>%
                                  group_by(date) %>%
                                  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                   HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Punggol)*100)

HourlyIndexReadingRate_Tuas <- index_data_hourly %>% filter(site=="Tuas") %>%
                               group_by(date,hour) %>%
                               dplyr::summarise(CountPerHour=n())%>%
                               group_by(date) %>%
                               dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                HourlyIndexReadingRate=MeanCountPerHour/nrow(servicepoint_available_Tuas)*100)

HourlyIndexReadingRate_Tuas_Customers <- index_data_hourly %>% filter(site=="Tuas") %>%
  group_by(MeterSerialNumber,date) %>%
  dplyr::summarise(CountPerDay=n())%>%
  group_by(MeterSerialNumber) %>%
  dplyr::summarise(IndexReadingRate=(sum(CountPerDay)/(24*30))*100)

# exclude block="NA", which is from Tuas and Whampoa
HourlyIndexReadingRateBlock <- index_data_hourly %>% filter(!block=="NA" & !site=="Whampoa") %>%
                                group_by(date,hour,block) %>%
                                dplyr::summarise(CountPerHour=n())%>%
                                group_by(date,block) %>%
                                dplyr::summarise(MeanCountPerHour=mean(CountPerHour))

servicepoint_available_Punggol_Blocks_Loop <- servicepoint_available_Punggol_Blocks[rep(seq_len(nrow(servicepoint_available_Punggol_Blocks)), 
                                                                                        times=nrow(HourlyIndexReadingRateBlock)/nrow(servicepoint_available_Punggol_Blocks)),] 

HourlyIndexReadingRate_Block <- cbind.data.frame(HourlyIndexReadingRateBlock,servicepoint_available_Punggol_Blocks_Loop) 
HourlyIndexReadingRate_Block[4] <- NULL  
HourlyIndexReadingRate_Block <- HourlyIndexReadingRate_Block %>%
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

save(f,x,y,DailyIndexReadingRate,HourlyIndexReadingRate,HourlyIndexReadingRate_Punggol,HourlyIndexReadingRate_Tuas,
     HourlyIndexReadingRate_Tuas_Customers,HourlyIndexReadingRate_Block,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last90days.RData")

time_taken <- proc.time() - ptm
ans <- paste("DailyHourlyIndexRate_last90days successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

HourlyIndexReadingRate_Punggol_xts <- xts(HourlyIndexReadingRate_Punggol$HourlyIndexReadingRate,HourlyIndexReadingRate_Punggol$date)
HourlyIndexReadingRate_Tuas_xts <- xts(HourlyIndexReadingRate_Tuas$HourlyIndexReadingRate,HourlyIndexReadingRate_Tuas$date)
HourlyIndexReadingRate_Site_xts <- cbind(HourlyIndexReadingRate_Punggol_xts,HourlyIndexReadingRate_Tuas_xts)
colnames(HourlyIndexReadingRate_Site_xts) <- c("Punggol","Tuas")
HourlyIndexReadingRatePerArea <- as.data.table(HourlyIndexReadingRate_Site_xts)
colnames(HourlyIndexReadingRatePerArea)[1] <- "Date"

HourlyIndexReadingRate_xts <- xts(HourlyIndexReadingRate$HourlyIndexReadingRate,HourlyIndexReadingRate$date)
DailyIndexReadingRate_xts <- xts(DailyIndexReadingRate$DailyIndexReadingRate,as.Date(DailyIndexReadingRate$date))
DailyHourlyIndexReadingRate_xts <- cbind(HourlyIndexReadingRate_xts,DailyIndexReadingRate_xts)
colnames(DailyHourlyIndexReadingRate_xts) <- c("HourlyIndexReadingRate","DailyIndexReadingRate")
DailyHourlyIndexReadingRate <- as.data.table(DailyHourlyIndexReadingRate_xts)
colnames(DailyHourlyIndexReadingRate)[1] <- "Date"

blocks <- c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")
for (i in 1:length(blocks)){
  assign(paste("HourlyIndexReadingRate_",blocks[i],sep=""), HourlyIndexReadingRate_Block %>% filter(block==blocks[i]))
}
HourlyIndexReadingRate_PG_B1_xts <- xts(HourlyIndexReadingRate_PG_B1$HourlyIndexReadingRate,HourlyIndexReadingRate_PG_B1$date)
HourlyIndexReadingRate_PG_B2_xts <- xts(HourlyIndexReadingRate_PG_B2$HourlyIndexReadingRate,HourlyIndexReadingRate_PG_B2$date)
HourlyIndexReadingRate_PG_B3_xts <- xts(HourlyIndexReadingRate_PG_B3$HourlyIndexReadingRate,HourlyIndexReadingRate_PG_B3$date)
HourlyIndexReadingRate_PG_B4_xts <- xts(HourlyIndexReadingRate_PG_B4$HourlyIndexReadingRate,HourlyIndexReadingRate_PG_B4$date)
HourlyIndexReadingRate_PG_B5_xts <- xts(HourlyIndexReadingRate_PG_B5$HourlyIndexReadingRate,HourlyIndexReadingRate_PG_B5$date)
HourlyIndexReadingRate_Block_xts <- cbind(HourlyIndexReadingRate_PG_B1_xts,HourlyIndexReadingRate_PG_B2_xts,
                                          HourlyIndexReadingRate_PG_B3_xts,HourlyIndexReadingRate_PG_B4_xts,
                                          HourlyIndexReadingRate_PG_B5_xts)
colnames(HourlyIndexReadingRate_Block_xts) <- blocks
HourlyIndexReadingRatePerBlock <- as.data.table(HourlyIndexReadingRate_Block_xts)
colnames(HourlyIndexReadingRatePerBlock)[1] <- "Date"