## To have a plot of both Index Availability and ConsumptionFiles Availability
## Explanation: 
## Index_Availability refers to the Number of Index available over the Total Number of Index expected per day. 
## With a total of 607 meters for both Punggol and Tuas before 1st April 2018, the expected Number of Index per day is 24x607=14568. 
## From 1st April 2018 onwards, we have an additional of 571 meters from Yuhua, so the expected Number of Index per day is then 24x(607+571)=28272.

## ConsumptionFiles_Availability refers to the Number of Consumption Files available over the Total of Number of 24 Consumption Files expected per day.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()
index_StartEnd <- as.data.frame(tbl(con,"index") %>% filter(date(current_index_date) >= "2016-03-15" & date(current_index_date) < today))

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="Yuhua")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index_servicepoint_meter <- inner_join(servicepoint_meter,index_StartEnd,by=c("id.x"="id_service_point"))

index_data_hourly <- index_servicepoint_meter %>%
  select(meter_sn,current_index_date,block,site) %>%
  mutate(Date=substr(current_index_date,1,10),hour=hour(current_index_date)) %>%
  dplyr::filter(meter_sn!="WJ003984A") # exclude AHL

index_data_hourly$Date <- as.Date(index_data_hourly$Date)

colnames(index_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","Date","hour")

Overall_Index_Count_VHF <- index_data_hourly %>% 
  dplyr::filter(site %in% c("Punggol","Tuas")) %>%
  dplyr::group_by(Date,hour) %>%
  dplyr::summarise(CountPerHour=n())%>%
  group_by(Date) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                   Index_Availability=round(MeanCountPerHour/(nrow(servicepoint_available_Punggol)+
                                                                nrow(servicepoint_available_Tuas))*100)) 
## Yuhua Index comes in 1st April 2018
Overall_Index_Count_LORA <- index_data_hourly %>% 
  dplyr::filter(site == "Yuhua" & Date >= "2018-04-01") %>%
  dplyr::group_by(Date,hour) %>%
  dplyr::summarise(CountPerHour=n())%>%
  group_by(Date) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                   Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Yuhua)*100)) 

Punggol_Index_Count <- index_data_hourly %>% dplyr::filter(site=="Punggol") %>%
                       group_by(Date,hour) %>%
                       dplyr::summarise(CountPerHour=n())%>%
                       group_by(Date) %>%
                       dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                        Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Punggol)*100)) %>%
                       dplyr::filter(Date < today)

Tuas_Index_Count <- index_data_hourly %>% dplyr::filter(site=="Tuas") %>%
                    group_by(Date,hour) %>%
                    dplyr::summarise(CountPerHour=n())%>%
                    group_by(Date) %>%
                    dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                    Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Tuas)*100)) %>%
                    dplyr::filter(Date < today)

Yuhua_Index_Count <- index_data_hourly %>% dplyr::filter(site=="Yuhua") %>%
                     group_by(Date,hour) %>%
                     dplyr::summarise(CountPerHour=n())%>%
                     group_by(Date) %>%
                     dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                     Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Yuhua)*100)) %>%
                     dplyr::filter(Date < today)

## Count number of Consumption files Per Day
directory_Consumption <- "/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Consumption"
Downloaded_ConsumptionFiles <- list.files(path=directory_Consumption, pattern="Consumption_", full.names=F, recursive=FALSE)
setwd(directory_Consumption)
FileCreation_Date <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles,
                             FUN=function(files){
                             read.csv(files,nrows=1,skip=2, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
                             }
))

FileCreation_Date$V2 <- substr(FileCreation_Date$V2,1,10)
FileCreation_Date$V2 <- dmy(FileCreation_Date$V2)

FileCreationDate <- FileCreation_Date %>% dplyr::select_("V2") 
colnames(FileCreationDate)[1] <- "Date"

ConsumptionFiles_Count <- FileCreationDate %>% dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n(),ConsumptionFiles_Availability=round(Count/24*100)) %>% dplyr::filter(Date < today)

Overall_IndexConsumptionFilesCount <- inner_join(Overall_Index_Count,ConsumptionFiles_Count,by="Date") %>%
                                      dplyr::select_("Date","Index_Availability","ConsumptionFiles_Availability") %>%
                                      dplyr::filter(Date < today) %>%
                                      dplyr::arrange(desc(Date))

Overall_IndexCount_xts <- xts(Overall_Index_Count$Index_Availability,Overall_Index_Count$Date)
ConsumptionFilesCount_xts <- xts(ConsumptionFiles_Count$ConsumptionFiles_Availability,ConsumptionFiles_Count$Date)
Overall_Index_ConsumptionFilesCount_xts <- cbind(Overall_IndexCount_xts,ConsumptionFilesCount_xts)
colnames(Overall_Index_ConsumptionFilesCount_xts)[1] <- "Index_Availability"
colnames(Overall_Index_ConsumptionFilesCount_xts)[2] <- "ConsumptionFiles_Availability"

save(Overall_IndexConsumptionFilesCount,Overall_Index_ConsumptionFilesCount_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DataQualityMonitoringInformation.RData")

Punggol_IndexCount_xts <- xts(Punggol_Index_Count$Index_Availability,Punggol_Index_Count$Date)
Tuas_IndexCount_xts <- xts(Tuas_Index_Count$Index_Availability,Tuas_Index_Count$Date)
Yuhua_IndexCount_xts <- xts(Yuhua_Index_Count$Index_Availability,Yuhua_Index_Count$Date)
PunggolTuasYuhua_IndexCount_xts <- cbind(cbind(Punggol_IndexCount_xts,Tuas_IndexCount_xts),Yuhua_IndexCount_xts)
colnames(PunggolTuasYuhua_IndexCount_xts)[1] <- "IndexAvailability_Punggol"
colnames(PunggolTuasYuhua_IndexCount_xts)[2] <- "IndexAvailability_Tuas"
colnames(PunggolTuasYuhua_IndexCount_xts)[3] <- "IndexAvailability_Yuhua"

save(PunggolTuasYuhua_IndexCount_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/PunggolTuasYuhuaIndexCounts.RData")

time_taken <- proc.time() - ptm
ans <- paste("DataQualityMonitoringInformation successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
