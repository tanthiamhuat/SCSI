# https://gps-coordinates.org/singapore-latitude.php

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

library(httr)
library(jsonlite)
library(lubridate)

require(xlsx)
NonDomesticCustomers <- read.xlsx("/srv/shiny-server/DataAnalyticsPortal/NonDomestic/NonDomesticCustomers.xlsx",sheetName="Sheet1")
NonDomesticCustomersPostalCode <- NonDomesticCustomers["Postal.Code"]
NonDomesticCustomersPostalCode[3,] <- "039593"
NonDomesticCustomersPostalCode[19,] <- "098271"
NonDomesticCustomersPostalCode[55,] <- "228211"
NonDomesticCustomersPostalCode[77,] <- "249715"
NonDomesticCustomersPostalCode[178,] <- "637578"
NonDomesticCustomersPostalCode[201,] <- "658713"
NonDomesticCustomersPostalCode[213,] <- "730845"

#url <- "https://developers.onemap.sg/commonapi/search?searchVal=650108&returnGeom=Y&getAddrDetails=Y&pageNum=1"

Long <- list()
Lat <- list()

for (i in 1:nrow(NonDomesticCustomersPostalCode))
{
  url <- paste("https://developers.onemap.sg/commonapi/search?searchVal=",NonDomesticCustomersPostalCode[i,],
               "&returnGeom=Y&getAddrDetails=Y&pageNum=1",sep="")
  r <- GET(url)
  r$status_code
  responseContent <- content(r, "text")
  json <- fromJSON(responseContent)
  json_data <- as.data.frame(unlist(json),stringsAsFactors = FALSE) 
  found <- as.numeric(json_data[1,])
  if (found > 1)
  {
    Long[i] <- json_data[which(rownames(json_data)=="results.LONGTITUDE1"),]
    Lat[i] <- json_data[which(rownames(json_data)=="results.LATITUDE1"),]
  } else {
    Long[i] <- json_data[which(rownames(json_data)=="results.LONGTITUDE"),]
    Lat[i] <- json_data[which(rownames(json_data)=="results.LATITUDE"),]
  } 
}

Longitude <- as.data.frame(unlist(Long))
Latitude <- as.data.frame(unlist(Lat))

NonDomesticCustomers_LongLat <-cbind(NonDomesticCustomers[,c(2,3,5)],Longitude,Latitude)
colnames(NonDomesticCustomers_LongLat) <- c("CompanyName","Address","Sector","Longitude","Latitude")
NonDomesticCustomers_LongLat$Type <- "Transmitter"

TuasReceiver <- c("TuasReceiver","30 TUAS AVE 10 S(639150)","",103.650253,1.332937,"Receiver")
PunggolReceiver_199C <- c("PunggolReceiver_199C","199C Punggol Field S(823199)","",103.905878,1.400575,"Receiver")
PunggolReceiver_274C <- c("PunggolReceiver_274C","274C PUNGGOL PLACE S(823274)","",103.902472,1.403054,"Receiver")
PunggolReceiver_613C <- c("PunggolReceiver_613C","613C Punggol Drive S(823613)","",103.908573,1.404082,"Receiver")
AMKReceiver_588D <- c("AngMoKioReceiver_588D","588D ANG MO KIO STREET 52 S(564588)","",103.852680,1.371990,"Receiver")
HougangReceiver_579 <- c("HougangReceiver_579","579 HOUGANG AVENUE 4, S(530579)","",103.886060,1.379040,"Receiver")
JurongWestReceiver_653C <- c("JurongWestReceiver_653C","653C JURONG WEST STREET 61 S(643653)","",103.701780, 1.335610,"Receiver")
BukitBatokReceiver_202 <- c("BukitBatokReceiver_202","202, BUKIT BATOK STREET 21, S(650202)","",103.748600,1.346500,"Receiver")

CompanyName <- c(TuasReceiver[1],PunggolReceiver_199C[1],PunggolReceiver_274C[1],PunggolReceiver_613C[1],
                 AMKReceiver_588D[1],HougangReceiver_579[1],JurongWestReceiver_653C[1],BukitBatokReceiver_202[1])
Address <- c(TuasReceiver[2],PunggolReceiver_199C[2],PunggolReceiver_274C[2],PunggolReceiver_613C[2],
             AMKReceiver_588D[2],HougangReceiver_579[2],JurongWestReceiver_653C[2],BukitBatokReceiver_202[2])
Sector <- c(rep("",8))
Longitude <- c(TuasReceiver[4],PunggolReceiver_199C[4],PunggolReceiver_274C[4],PunggolReceiver_613C[4],
               AMKReceiver_588D[4],HougangReceiver_579[4],JurongWestReceiver_653C[4],BukitBatokReceiver_202[4])
Latitude <- c(TuasReceiver[5],PunggolReceiver_199C[5],PunggolReceiver_274C[5],PunggolReceiver_613C[5],
              AMKReceiver_588D[5],HougangReceiver_579[5],JurongWestReceiver_653C[5],BukitBatokReceiver_202[5])
Type <- c(rep("Receiver",8))

Receivers <- data.frame(CompanyName,Address,Sector,Longitude,Latitude,Type)

NonDomesticCustomersReceivers_LongLat <- rbind(NonDomesticCustomers_LongLat,Receivers)

NonDomesticCustomersReceivers_LongLat$Longitude <- as.numeric(NonDomesticCustomersReceivers_LongLat$Longitude)
NonDomesticCustomersReceivers_LongLat$Latitude <- as.numeric(NonDomesticCustomersReceivers_LongLat$Latitude)

save(NonDomesticCustomersReceivers_LongLat,file=paste0(path,'data/NonDomestic.RData'))

