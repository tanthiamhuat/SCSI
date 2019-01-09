rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst,geosphere)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last30days.RData")

## -------- Data Quality ------------- ##
## in AWS it is showings the Map, while in GDC it is showing the Plotly plot
AverageDailyReadingRate_Punggol <- DailyIndexReadingRate_Punggol %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

AverageHourlyReadingRate_Yuhua <- NA

QuantityNoDataMeter <- BillableMeters[nrow(BillableMeters),ncol(BillableMeters)]

AverageHourlyReadingRate <- HourlyIndexReadingRate_PunggolBlocks %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

blocks <- sort(unique(HourlyIndexReadingRate_PunggolBlocks$block))
ReadingRate_blocks <- list()
for (i in 1:length(blocks)){
  ReadingRate_blocks[[i]] <- AverageHourlyReadingRate %>% filter(block==blocks[i]) %>% dplyr::select_("AverageHourlyReadingRate")
}
ReadingRate_blocks <- c(unlist(ReadingRate_blocks))

## http://www.latlong.net/convert-address-to-lat-long.html
## PG_B1, 103C Edgefield Plains, Lat=1.397414,Lon=103.904557
## PG_B2, 199C Punggol Field, Lat=1.400575,Lon=103.905878
## PG_B3, 266A Punggol Way,Lat=1.405382,Lon=103.897793
## PG_B4, 613C, Punggol Drive, Lat=1.404082,Lon=103.908573
## PG_B5, 624C Punggol Central, Lat=1.399853,Lon=103.911212
## Yuhua, Latitude=1.3245370, Longitude=103.7425669
PG_103C_LongLat <- c(103.904557,1.397414)
PG_199C_LongLat <- c(103.905878,1.400575)
PG_266A_LongLat <- c(103.897793,1.405382)
PG_613C_LongLat <- c(103.908573,1.404082)
PG_624C_LongLat <- c(103.911212,1.399853)
Punggol_LongLat <- rbind(PG_103C_LongLat,PG_199C_LongLat,PG_266A_LongLat,PG_613C_LongLat,PG_624C_LongLat) %>% as.data.frame()

# Freight Links E-Logistic Technopark 30 TuasAve 10 S(639150)	Latitude : 1.332937, Longitude : 103.650253
# Blk 199C Punggol Field S(823199) Latitude : 1.400575, Longitude : 103.905878
# Blk 274C PUNGGOL PLACE S(823274) Latitude : 1.403054, Longitude : 103.902472
# Blk 613C Punggol Drive S(823613) Latitude : 1.404082, Longitude : 103.908573
Receiver_Tuas_LongLat <- c(103.650253,1.332937)
Receiver_199C_LongLat <- c(103.905878,1.400575)
Receiver_274C_LongLat <- c(103.902472,1.403054)
Receiver_613C_LongLat <- c(103.908573,1.404082)

PG_103C_Distance=round(as.numeric(distm(PG_103C_LongLat,Receiver_199C_LongLat,fun = distHaversine)))
PG_199C_Distance=round(as.numeric(distm(PG_199C_LongLat,Receiver_199C_LongLat,fun = distHaversine)))
PG_266A_Distance=round(as.numeric(distm(PG_266A_LongLat,Receiver_274C_LongLat,fun = distHaversine)))
PG_613C_Distance=round(as.numeric(distm(PG_613C_LongLat,Receiver_613C_LongLat,fun = distHaversine)))
PG_624C_Distance=round(as.numeric(distm(PG_624C_LongLat,Receiver_613C_LongLat,fun = distHaversine)))
PG_Distance <- c(PG_103C_Distance,PG_199C_Distance,PG_266A_Distance,PG_613C_Distance,PG_624C_Distance)

Tuas_Customers <- read_excel("/srv/shiny-server/DataAnalyticsPortal/data/Tuas_Customers.xlsx",1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude")

Tuas <- inner_join(Tuas_Customers,HourlyIndexReadingRate_Tuas_Customers,by="MeterSerialNumber") %>%
  dplyr::group_by(customer,longitude,latitude) %>%
  dplyr::summarise(ReadingRate=round(mean(IndexReadingRate),2)) %>% as.data.frame()
Tuas$longitude <- as.numeric(Tuas$longitude)
Tuas$latitude <- as.numeric(Tuas$latitude)
Tuas <- as.data.frame(Tuas)

if (nrow(Tuas)==0) {
  Tuas <- Tuas_Customers %>% dplyr::select_("customer","longitude","latitude")
  Tuas$ReadingRate <- rep(0,nrow(Tuas_Customers))
}

Punggol <- data.frame(customer = c(blocks),
                      longitude = c(Punggol_LongLat$V1),
                      latitude = c(Punggol_LongLat$V2),
                      ReadingRate=ReadingRate_blocks)
rownames(Punggol) <- c(seq(1,length(ReadingRate_blocks)))

PunggolTuas <- rbind(Punggol,Tuas)
PunggolTuas <- as.data.frame(PunggolTuas)

PunggolTuas <- PunggolTuas %>% mutate(ReadingRatesCat=ifelse(ReadingRate < 70,'red',
                                                      ifelse(ReadingRate >= 70 & ReadingRate < 90,'orange',
                                                      ifelse(ReadingRate >= 90, 'green', NA))))

ReadingRateIcons <- awesomeIconList(
  green = makeAwesomeIcon(icon='user', library='fa', markerColor = 'green'),
  orange = makeAwesomeIcon(icon='user', library='fa', markerColor = 'orange'),
  red = makeAwesomeIcon(icon='user', library='fa', markerColor = 'red'))

## below is for the GDC to plot the Plotly chart
Tuas_CustomersDistance <- read_excel("/srv/shiny-server/DataAnalyticsPortal/data/Tuas_Customers.xlsx",1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude") %>%
  dplyr::mutate(Distance=round(as.numeric(distm(Receiver_Tuas_LongLat,cbind(longitude,latitude),fun = distHaversine)))) %>% 
  dplyr::select_("customer","Distance") %>%
  as.data.frame() %>% unique()
## distance is in meter

Punggol_DistanceReadingRate <- data.frame(customer = c(blocks),
                                           ReadingRate=ReadingRate_blocks,
                                           Distance=PG_Distance)

Tuas_DistanceReadingRate <- inner_join(Tuas,Tuas_CustomersDistance,by="customer") %>% 
                            dplyr::select_("customer","ReadingRate","Distance") %>%
                            unique()

PunggolTuas_DistanceReadingRate <- rbind(Punggol_DistanceReadingRate,Tuas_DistanceReadingRate)
PunggolTuas_DistanceReadingRate$site <- c(rep("Punggol",5),rep("Tuas",30))
 

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Dashboard successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
