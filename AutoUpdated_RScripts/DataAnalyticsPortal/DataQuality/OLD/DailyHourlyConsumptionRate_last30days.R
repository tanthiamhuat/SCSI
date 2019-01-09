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

last30days <- today()-30
today <- today()
consumption_last30days <- as.data.frame(tbl(con,"consumption") %>% filter(date(date_consumption) >= last30days &
                                                                          date(date_consumption) < today &
                                                                          status=="Interpolated value"))

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

consumption_servicepoint_meter <- inner_join(servicepoint_meter,consumption_last30days,by=c("id.x"="id_service_point"))

consumption_data_hourly <- consumption_servicepoint_meter %>%
                           select(meter_sn,date_consumption,block,site) %>%
                           mutate(date=substr(date_consumption,1,10),hour=hour(date_consumption))
consumption_data_hourly$date <- as.Date(consumption_data_hourly$date)

colnames(consumption_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","date","hour")

DailyConsumptionReadingRate <- consumption_data_hourly %>% 
                               group_by(date) %>%
                               dplyr::summarise(TotalMeters=sum(n_distinct(MeterSerialNumber))) %>%
                               dplyr::mutate(DailyIndexReadingRate=round(TotalMeters/nrow(servicepoint_available)*100,2))
  
HourlyConsumptionReadingRate <- consumption_data_hourly %>% 
                                group_by(date,hour) %>%
                                dplyr::summarise(CountPerHour=n())%>%
                                group_by(date) %>%
                                dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                           HourlyConsumptionReadingRate=MeanCountPerHour/nrow(servicepoint_available)*100)

HourlyConsumptionReadingRate_Punggol <- consumption_data_hourly %>% filter(site=="Punggol") %>%
                                        group_by(date,hour) %>%
                                        dplyr::summarise(CountPerHour=n())%>%
                                        group_by(date) %>%
                                        dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                         HourlyConsumptionReadingRate=MeanCountPerHour/nrow(servicepoint_available_Punggol)*100)

HourlyConsumptionReadingRate_Punggol_Customers <- consumption_data_hourly %>% filter(site=="Punggol") %>%
                                                  group_by(MeterSerialNumber,date) %>%
                                                  dplyr::summarise(CountPerDay=n())%>%
                                                  group_by(MeterSerialNumber) %>%
                                                  dplyr::summarise(ConsumptionReadingRate=(sum(CountPerDay)/(24*30))*100)

HourlyConsumptionReadingRate_Tuas <- consumption_data_hourly %>% filter(site=="Tuas") %>%
                                     group_by(date,hour) %>%
                                     dplyr::summarise(CountPerHour=n())%>%
                                     group_by(date) %>%
                                     dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                                      HourlyConsumptionReadingRate=MeanCountPerHour/nrow(servicepoint_available_Tuas)*100)

HourlyConsumptionReadingRate_Tuas_Customers <- consumption_data_hourly %>% filter(site=="Tuas") %>%
                                               group_by(MeterSerialNumber,date) %>%
                                               dplyr::summarise(CountPerDay=n())%>%
                                               group_by(MeterSerialNumber) %>%
                                               dplyr::summarise(ConsumptionReadingRate=(sum(CountPerDay)/(24*30))*100)

# exclude block="NA", which is from Tuas and Whampoa
HourlyConsumptionReadingRateBlock <- consumption_data_hourly %>% filter(!block=="NA" & !site=="Whampoa") %>%
                                     group_by(date,hour,block) %>%
                                     dplyr::summarise(CountPerHour=n())%>%
                                     group_by(date,block) %>%
                                     dplyr::summarise(MeanCountPerHour=mean(CountPerHour))

servicepoint_available_Punggol_Blocks_Loop <- servicepoint_available_Punggol_Blocks[rep(seq_len(nrow(servicepoint_available_Punggol_Blocks)), 
                                                                                        times=nrow(HourlyConsumptionReadingRateBlock)/nrow(servicepoint_available_Punggol_Blocks)),] 

HourlyConsumptionReadingRate_Block <- cbind.data.frame(HourlyConsumptionReadingRateBlock,servicepoint_available_Punggol_Blocks_Loop) 
HourlyConsumptionReadingRate_Block[4] <- NULL  
HourlyConsumptionReadingRate_Block <- HourlyConsumptionReadingRate_Block %>%
                                dplyr::mutate(HourlyConsumptionReadingRate=round(MeanCountPerHour/MeterCount*100,2))

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

save(f,x,y,DailyConsumptionReadingRate,HourlyConsumptionReadingRate,HourlyConsumptionReadingRate_Punggol,HourlyConsumptionReadingRate_Tuas,
     HourlyConsumptionReadingRate_Tuas_Customers,HourlyConsumptionReadingRate_Block,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyConsumptionRate_last30days.RData")

time_taken <- proc.time() - ptm
ans <- paste("DailyHourlyConsumptionRate_last30days successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)