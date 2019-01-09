## To have a plot of both Index Availability and ConsumptionFiles Availability
## Explanation: 
## Index_Availability refers to the Number of Index available over the Total Number of Index expected per day. 
## With a total of 607 meters for both Punggol and Tuas before 1st April 2018, the expected Number of Index per day is 24x607=14568. 
## From 1st April 2018 onwards, we have an additional of 571 meters from Yuhua, so the expected Number of Index per day is then 24x(607+571)=28272.

## ConsumptionFiles_Availability refers to the Number of Consumption Files available over the Total of Number of 24 Consumption Files expected per day.
## Below plots are for year 2018 onwards

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

index_2018 <- as.data.frame(tbl(con,"index")) 

index_2018 <- index_2018 %>% dplyr::filter(date(current_index_date) >= "2018-01-01" & date(current_index_date) < today() & id_service_point !=601)

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          dplyr::filter(!service_point_sn %in% c("3100507837M","3100507837B","3100660792"))
servicepoint_available_Punggol <- servicepoint_available %>% dplyr::filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% dplyr::filter(site=="Yuhua")
servicepoint_available_Tuas <- servicepoint_available %>% dplyr::filter(site=="Tuas")

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")

servicepoint_meter <- inner_join(servicepoint_available,meter,by=c("service_point_sn"="id_real_estate"))

index2018_servicepoint_meter <- inner_join(servicepoint_meter,index_2018,by=c("id.x"="id_service_point"))

index2018_data_hourly <- index2018_servicepoint_meter %>%
  select(meter_sn,current_index_date,block,site) %>%
  mutate(date=substr(current_index_date,1,10),hour=hour(current_index_date)) %>%
  dplyr::filter(meter_sn!="WJ003984A") # exclude AHL

index2018_data_hourly$date <- as.Date(index2018_data_hourly$date)

colnames(index2018_data_hourly) <- c("MeterSerialNumber","ReadingDate","block","site","Date","hour")

## before Yuhua Index comes in 1st April 2018
Overall_Index_Count_Before <- index2018_data_hourly %>% 
  dplyr::group_by(Date,hour) %>%
  dplyr::summarise(CountPerHour=n())%>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                   Index_Availability=round(MeanCountPerHour/(nrow(servicepoint_available_Punggol)+
                                                              nrow(servicepoint_available_Tuas))*100)) %>%
  dplyr::filter(Date < "2018-04-01")

## After Yuhua Index comes in 1st April 2018
Overall_Index_Count_After <- index2018_data_hourly %>% 
  dplyr::group_by(Date,hour) %>%
  dplyr::summarise(CountPerHour=n())%>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                   Index_Availability=round(MeanCountPerHour/(nrow(servicepoint_available_Punggol)+
                                                              nrow(servicepoint_available_Tuas)+nrow(servicepoint_available_Yuhua))*100)) %>%
  dplyr::filter(Date >= "2018-04-01")

Overall_Index_Count <- rbind(Overall_Index_Count_Before,Overall_Index_Count_After)
Punggol_Index_Count <- index2018_data_hourly %>% dplyr::filter(site=="Punggol") %>%
                       dplyr::group_by(Date,hour) %>%
                       dplyr::summarise(CountPerHour=n())%>%
                       dplyr::group_by(Date) %>%
                       dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                                        Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Punggol)*100)) %>%
                       dplyr::filter(Date < today())

Tuas_Index_Count <- index2018_data_hourly %>% dplyr::filter(site=="Tuas") %>%
                    dplyr::group_by(Date,hour) %>%
                    dplyr::summarise(CountPerHour=n())%>%
                    dplyr::group_by(Date) %>%
                    dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                    Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Tuas)*100)) %>%
                    dplyr::filter(Date < today())

Yuhua_Index_Count <- index2018_data_hourly %>% dplyr::filter(site=="Yuhua") %>%
                     dplyr::group_by(Date,hour) %>%
                     dplyr::summarise(CountPerHour=n())%>%
                     dplyr::group_by(Date) %>%
                     dplyr::summarise(MeanCountPerHour=mean(CountPerHour),
                     Index_Availability=round(MeanCountPerHour/nrow(servicepoint_available_Yuhua)*100)) %>%
                     dplyr::filter(Date < today())

## Count number of Consumption files Per Day
directory_Consumption <- "/srv/shiny-server/DataAnalyticsPortal/data/SFTP/Consumption"
Downloaded_ConsumptionFiles_2018 <- list.files(path=directory_Consumption, pattern="Consumption_18", full.names=F, recursive=FALSE)
setwd(directory_Consumption)
FileCreation_Date <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles_2018,
                             FUN=function(files){
                             read.csv(files,nrows=1,skip=2, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
                             }
))

FileCreation_Date$V2 <- substr(FileCreation_Date$V2,1,10)
FileCreation_Date$V2 <- dmy(FileCreation_Date$V2)

FileCreationDate <- FileCreation_Date %>% dplyr::select_("V2") 
colnames(FileCreationDate)[1] <- "Date"

ConsumptionFiles_Count <- FileCreationDate %>% dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n(),ConsumptionFiles_Availability=round(Count/24*100)) %>% dplyr::filter(Date < today())

Overall_Index_Count$Date <- as.Date(Overall_Index_Count$Date)

Overall_IndexConsumptionFilesCount <- inner_join(Overall_Index_Count,ConsumptionFiles_Count,by="Date") %>%
                                      dplyr::select_("Date","Index_Availability","ConsumptionFiles_Availability") %>%
                                      dplyr::filter(Date < today()) %>%
                                      dplyr::arrange(desc(Date))

Overall_IndexCount_xts <- xts(Overall_Index_Count$Index_Availability,Overall_Index_Count$Date)
ConsumptionFilesCount_xts <- xts(ConsumptionFiles_Count$ConsumptionFiles_Availability,ConsumptionFiles_Count$Date)
Overall_Index_ConsumptionFilesCount_xts <- cbind(Overall_IndexCount_xts,ConsumptionFilesCount_xts)
colnames(Overall_Index_ConsumptionFilesCount_xts)[1] <- "Index_Availability"
colnames(Overall_Index_ConsumptionFilesCount_xts)[2] <- "ConsumptionFiles_Availability"

save(Overall_IndexConsumptionFilesCount,Overall_Index_ConsumptionFilesCount_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DataQualityMonitoringInformation.RData")

Punggol_Index_Count$Date <- as.Date(Punggol_Index_Count$Date)
Punggol_IndexCount_xts <- xts(Punggol_Index_Count$Index_Availability,Punggol_Index_Count$Date)

Tuas_Index_Count$Date <- as.Date(Tuas_Index_Count$Date)
Tuas_IndexCount_xts <- xts(Tuas_Index_Count$Index_Availability,Tuas_Index_Count$Date)

Yuhua_Index_Count$Date <- as.Date(Yuhua_Index_Count$Date)
Yuhua_IndexCount_xts <- xts(Yuhua_Index_Count$Index_Availability,Yuhua_Index_Count$Date)
PunggolTuasYuhua_IndexCount_xts <- cbind(cbind(Punggol_IndexCount_xts,Tuas_IndexCount_xts),Yuhua_IndexCount_xts)
colnames(PunggolTuasYuhua_IndexCount_xts)[1] <- "IndexAvailability_Punggol"
colnames(PunggolTuasYuhua_IndexCount_xts)[2] <- "IndexAvailability_Tuas"
colnames(PunggolTuasYuhua_IndexCount_xts)[3] <- "IndexAvailability_Yuhua"

save(PunggolTuasYuhua_IndexCount_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/PunggolTuasYuhuaIndexCounts.RData")

time_taken <- proc.time() - ptm
ans <- paste("DataQualityMonitoringInformation_V3 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
