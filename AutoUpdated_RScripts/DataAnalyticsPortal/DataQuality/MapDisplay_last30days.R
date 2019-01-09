rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr,fst,geosphere)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

load(paste0(path,'data/DailyHourlyIndexRate_last30days.RData'))

## -------- Data Quality ------------- ##
## in AWS it is showings the Map, while in GDC it is showing the Plotly plot
AverageHourlyReadingRate_PG <- HourlyIndexReadingRate_PunggolBlocks %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

blocks_PG <- sort(unique(HourlyIndexReadingRate_PunggolBlocks$block))
ReadingRate_blocks_PG <- list()
for (i in 1:length(blocks_PG)){
  ReadingRate_blocks_PG[[i]] <- AverageHourlyReadingRate_PG %>% dplyr::filter(block==blocks_PG[i]) %>% dplyr::select_("AverageHourlyReadingRate")
}
ReadingRate_blocks_PG <- c(unlist(ReadingRate_blocks_PG))

AverageHourlyReadingRate_YH <- HourlyIndexReadingRate_YuhuaBlocks %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(AverageHourlyReadingRate=round(mean(HourlyIndexReadingRate),2))

blocks_YH <- sort(unique(HourlyIndexReadingRate_YuhuaBlocks$block))
ReadingRate_blocks_YH <- list()
for (i in 1:length(blocks_YH)){
  ReadingRate_blocks_YH[[i]] <- AverageHourlyReadingRate_YH %>% dplyr::filter(block==blocks_YH[i]) %>% dplyr::select_("AverageHourlyReadingRate")
}
ReadingRate_blocks_YH <- c(unlist(ReadingRate_blocks_YH))

## http://www.latlong.net/convert-address-to-lat-long.html
## PG_B1, 103C Edgefield Plains, Lat=1.397414,Lon=103.904557
## PG_B2, 199C Punggol Field, Lat=1.400575,Lon=103.905878
## PG_B3, 266A Punggol Way,Lat=1.405382,Lon=103.897793
## PG_B4, 613C, Punggol Drive, Lat=1.404082,Lon=103.908573
## PG_B5, 624C Punggol Central, Lat=1.399853,Lon=103.911212
## YH_B1, 213, Jurong East ST 21, Lat=1.339035,Lon=103.739732
## YH_B2, 214, Jurong East ST 21, Lat=1.339259,Lon=103.739099
## YH_B3, 222, Jurong East ST 21, Lat=1.341018,Lon=103.735637
## YH_B4, 224, Jurong East ST 21, Lat=1.341689,Lon=103.736746
## YH_B5, 228, Jurong East ST 21, Lat=1.342571,Lon=103.736293
## YH_B6, 234, Jurong East ST 21, Lat=1.339922,Lon=103.741611
## YH_B7, 237, Jurong East ST 21, Lat=1.340712,Lon=103.741181

## Yuhua, Latitude=1.3245370, Longitude=103.7425669
# 212, Jurong East ST 21, Lat=1.339110,Lon=103.740370
# 220, Jurong East ST 21, Lat=1.341090,Lon=103.737480

PG_103C_LongLat <- c(103.904557,1.397414)
PG_199C_LongLat <- c(103.905878,1.400575)
PG_266A_LongLat <- c(103.897793,1.405382)
PG_613C_LongLat <- c(103.908573,1.404082)
PG_624C_LongLat <- c(103.911212,1.399853)
Punggol_LongLat <- rbind(PG_103C_LongLat,PG_199C_LongLat,PG_266A_LongLat,PG_613C_LongLat,PG_624C_LongLat) %>% as.data.frame()

YH_213_LongLat <- c(103.739732,1.339035)
YH_214_LongLat <- c(103.739099,1.339259)
YH_222_LongLat <- c(103.735637,1.341018)
YH_224_LongLat <- c(103.736746,1.341689)
YH_228_LongLat <- c(103.736293,1.342571)
YH_234_LongLat <- c(103.741611,1.339922)
YH_237_LongLat <- c(103.741181,1.340712)

YH_212_LongLat <- c(103.740370,1.339110)
YH_217_LongLat <- c(103.738014,1.339997)
YH_220_LongLat <- c(103.737480,1.341090)

Yuhua_LongLat <- rbind(YH_213_LongLat,YH_214_LongLat,YH_222_LongLat,YH_224_LongLat,YH_228_LongLat,YH_234_LongLat,YH_237_LongLat) %>% as.data.frame()

# Freight Links E-Logistic Technopark 30 TuasAve 10 S(639150)	Latitude : 1.332937, Longitude : 103.650253
# Blk 199C Punggol Field S(823199) Latitude : 1.400575, Longitude : 103.905878
# Blk 274C PUNGGOL PLACE S(823274) Latitude : 1.403054, Longitude : 103.902472
# Blk 613C Punggol Drive S(823613) Latitude : 1.404082, Longitude : 103.908573
Receiver_Tuas <- c(103.650253,1.332937)
Receiver_199C <- c(103.905878+0.0001,1.400575+0.0001)
Receiver_274C <- c(103.902472,1.403054)
Receiver_613C <- c(103.908573+0.0001,1.404082+0.0001)

Receiver_212 <- YH_212_LongLat 
Receiver_217 <- YH_217_LongLat 
Receiver_220 <- YH_220_LongLat 

Receivers_LongLat <- rbind(Receiver_Tuas,Receiver_199C,Receiver_274C,Receiver_613C,Receiver_212,Receiver_217,Receiver_220) %>% as.data.frame()

PG_103C_Distance=round(as.numeric(distm(PG_103C_LongLat,Receiver_199C,fun = distHaversine)))
PG_199C_Distance=round(as.numeric(distm(PG_199C_LongLat,Receiver_199C,fun = distHaversine)))
PG_266A_Distance=round(as.numeric(distm(PG_266A_LongLat,Receiver_274C,fun = distHaversine)))
PG_613C_Distance=round(as.numeric(distm(PG_613C_LongLat,Receiver_613C,fun = distHaversine)))
PG_624C_Distance=round(as.numeric(distm(PG_624C_LongLat,Receiver_613C,fun = distHaversine)))
PG_Distance <- c(PG_103C_Distance,PG_199C_Distance,PG_266A_Distance,PG_613C_Distance,PG_624C_Distance)

YH_213_Distance=round(as.numeric(distm(YH_213_LongLat,Receiver_212,fun = distHaversine)))
YH_214_Distance=round(as.numeric(distm(YH_214_LongLat,Receiver_217,fun = distHaversine)))
YH_222_Distance=round(as.numeric(distm(YH_222_LongLat,Receiver_220,fun = distHaversine)))
YH_224_Distance=round(as.numeric(distm(YH_224_LongLat,Receiver_220,fun = distHaversine)))
YH_228_Distance=round(as.numeric(distm(YH_228_LongLat,Receiver_220,fun = distHaversine)))
YH_234_Distance=round(as.numeric(distm(YH_234_LongLat,Receiver_212,fun = distHaversine)))
YH_237_Distance=round(as.numeric(distm(YH_237_LongLat,Receiver_212,fun = distHaversine)))
YH_Distance <- c(YH_213_Distance,YH_214_Distance,YH_222_Distance,YH_224_Distance,YH_228_Distance,YH_234_Distance,YH_237_Distance)

Tuas_Customers <- read_excel(paste0(path,'data/Tuas_Customers.xlsx'),1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude")

Tuas <- inner_join(Tuas_Customers,HourlyIndexReadingRate_Tuas_Customers,by="MeterSerialNumber") %>%
  dplyr::group_by(customer,longitude,latitude) %>%
  dplyr::summarise(ReadingRate=round(mean(IndexReadingRate))) %>% as.data.frame()
Tuas$longitude <- as.numeric(Tuas$longitude)
Tuas$latitude <- as.numeric(Tuas$latitude)
Tuas <- as.data.frame(Tuas)

if (nrow(Tuas)==0) {
  Tuas <- Tuas_Customers %>% dplyr::select_("customer","longitude","latitude")
  Tuas$ReadingRate <- rep(0,nrow(Tuas_Customers))
}

Punggol <- data.frame(customer = c(blocks_PG),
                      longitude = c(Punggol_LongLat$V1),
                      latitude = c(Punggol_LongLat$V2),
                      ReadingRate=round(ReadingRate_blocks_PG))
rownames(Punggol) <- c(seq(1,length(ReadingRate_blocks_PG)))

Yuhua <- data.frame(customer = c(blocks_YH),
                    longitude = c(Yuhua_LongLat$V1),
                    latitude = c(Yuhua_LongLat$V2),
                    ReadingRate=round(ReadingRate_blocks_YH))
rownames(Yuhua) <- c(seq(1,length(ReadingRate_blocks_YH)))

PunggolYuhuaTuas <- rbind(rbind(Punggol,Yuhua),Tuas)
PunggolYuhuaTuas <- as.data.frame(PunggolYuhuaTuas)

PunggolYuhuaTuas <- PunggolYuhuaTuas %>% mutate(ReadingRatesCat=ifelse(ReadingRate < 70,'red',
                                                                ifelse(ReadingRate >= 70 & ReadingRate < 90,'orange',
                                                                ifelse(ReadingRate >= 90, 'green', NA))))
PunggolYuhuaTuas$Type <- "Transmitter"

## Receivers information
## Name        Block          Location
## 154350409   Freight Links  Tuas
## 154350410   199C           Punggol
## 154350406   274C           Punggol
## 154350403   613C           Punggol

Receivers <- data.frame(customer = rownames(Receivers_LongLat),
                        longitude = c(Receivers_LongLat$V1),
                        latitude = c(Receivers_LongLat$V2),
                        ReadingRate = c("154350409","154350410","154350406","154350403","xx1234xxx","yy1234yyy","yy4321yyy"),
                        ReadingRatesCat = rep("",7),
                        Type = rep("Receiver",7))

PunggolYuhuaTuas_TransmitterReceiver <- rbind(PunggolYuhuaTuas,Receivers)

## below is for the GDC to plot the Plotly chart
Tuas_CustomersDistance <- read_excel(paste0(path,'data/Tuas_Customers.xlsx'),1) %>%
  dplyr::select_("customer","service_point_sn","MeterSerialNumber","longitude","latitude") %>%
  dplyr::mutate(Distance=round(as.numeric(distm(Receiver_Tuas,cbind(longitude,latitude),fun = distHaversine)))) %>% 
  dplyr::select_("customer","Distance") %>%
  as.data.frame() %>% unique()
## distance is in meter

Punggol_DistanceReadingRate <- data.frame(customer = c(blocks_PG),
                                           ReadingRate=ReadingRate_blocks_PG,
                                           Distance=PG_Distance)

Yuhua_DistanceReadingRate <- data.frame(customer = c(blocks_YH),
                                          ReadingRate=ReadingRate_blocks_YH,
                                          Distance=YH_Distance)

Tuas_DistanceReadingRate <- inner_join(Tuas,Tuas_CustomersDistance,by="customer") %>% 
                            dplyr::select_("customer","ReadingRate","Distance") %>%
                            unique()

PunggolYuhuaTuas_DistanceReadingRate <- rbind(Punggol_DistanceReadingRate,Yuhua_DistanceReadingRate,Tuas_DistanceReadingRate)
PunggolYuhuaTuas_DistanceReadingRate$site <- c(rep("Punggol",nrow(Punggol_DistanceReadingRate)),rep("Yuhua",nrow(Yuhua_DistanceReadingRate)),
                                               rep("Tuas",nrow(Tuas_DistanceReadingRate)))
 
MapDisplayTable <- PunggolYuhuaTuas_TransmitterReceiver %>% dplyr::filter(Type=="Transmitter") %>%
  dplyr::select_("customer","ReadingRate") 
MapDisplayTable$ReadingRate <- as.integer(MapDisplayTable$ReadingRate)
MapDisplayTable_PunggolYuhua <- MapDisplayTable[1:12,]
MapDisplayTable$ReadingRate <- as.integer(MapDisplayTable$ReadingRate)
MapDisplayTable_Tuas <- MapDisplayTable[13:nrow(MapDisplayTable),] %>%
  dplyr::summarise(ReadingRate=round(mean(ReadingRate)))
MapDisplayTable_Tuas <- data.frame(customer="Tuas",ReadingRate=MapDisplayTable_Tuas$ReadingRate)
MapDisplayTable_PunggolYuhuaTuas_last30days <- rbind(MapDisplayTable_PunggolYuhua,MapDisplayTable_Tuas)
colnames(MapDisplayTable_PunggolYuhuaTuas_last30days)[1] <- "block"

Updated_DateTime_MapDisplay <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")
  
save(PunggolYuhuaTuas,PunggolYuhuaTuas_TransmitterReceiver,
     PunggolYuhuaTuas_DistanceReadingRate,MapDisplayTable_PunggolYuhuaTuas_last30days,
     Updated_DateTime_MapDisplay,
     file=paste0(path,'data/MapDisplay_last30days.RData'))

time_taken <- proc.time() - ptm
ans <- paste("MapDisplay_last30days successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)