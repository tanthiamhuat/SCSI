rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last30days <- today()-30
today <- today()
index_last30days <- as.data.frame(tbl(con,"index") %>% filter(date(current_index_date) >= last30days &
                                                              date(current_index_date) < today))

#index_last30days <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/DB_Index_last6months.fst") 

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

index_servicepoint_meter <- inner_join(servicepoint_meter,index_last30days,by=c("id.x"="id_service_point"))

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

PunggolTuas <- inner_join(HourlyIndexReadingRate_Punggol,HourlyIndexReadingRate_Tuas,by="date")
PunggolTuas <- PunggolTuas[,c(1,3,5)]
colnames(PunggolTuas) <- c("Date","HourlyIndexReadingRate_Punggol","HourlyIndexReadingRate_Tuas")
PunggolTuas_Overall <- inner_join(PunggolTuas,HourlyIndexReadingRate,by=c("Date"="date"))
PunggolTuas_Overall[4] <- NULL
colnames(PunggolTuas_Overall[4]) <- "HourlyIndexReadingRate_Overall"
write.csv(PunggolTuas_Overall,file="HourlyIndexReadingRate.csv")

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

Updated_string <- paste("Last Updated on",now())

save(f,x,y,DailyIndexReadingRate,HourlyIndexReadingRate,HourlyIndexReadingRate_Punggol,HourlyIndexReadingRate_Tuas,
     HourlyIndexReadingRate_Tuas_Customers,HourlyIndexReadingRate_Block,Updated_string,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last30days.RData")

time_taken <- proc.time() - ptm
ans <- paste("DailyHourlyIndexRate_last30days successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)