## To have a plot of both Index Availability and CCFiles Availability

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

today <- today()

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="YUHUA")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

## Count Percentage of Index Available Per Day
load("/srv/shiny-server/DataAnalyticsPortal/data/RawIndex.RData")
RawIndexCount <- RawIndex %>% dplyr::mutate(Date=date(ReadingDate)) %>%
                 dplyr::group_by(Date) %>%
                 dplyr::summarise(Count=n(),
                                  IndexAvailability=round(Count/(24*(nrow(servicepoint_available_Punggol)+nrow(servicepoint_available_Tuas)))*100)) %>% 
                 dplyr::filter(Date >= "2016-03-15" & Date < today-1)

## Count number of CC files Per Day
directory_CC <- "/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Consumption"
Downloaded_ConsumptionFiles <- list.files(path=directory_CC, pattern="Consumption_", full.names=F, recursive=FALSE)
setwd(directory_CC)
FileCreation_Date <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles,
                             FUN=function(files){
                             read.csv(files,nrows=1,skip=2, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
                             }
)) ## retrieve information on File Creation Date

FileCreation_Date$V2 <- substr(FileCreation_Date$V2,1,10)
FileCreation_Date$V2 <- dmy(FileCreation_Date$V2)

FileCreationDate <- FileCreation_Date %>% dplyr::select_("V2") 
colnames(FileCreationDate)[1] <- "Date"

CCFilesCount <- FileCreationDate %>% dplyr::group_by(Date) %>%
                dplyr::summarise(Count=n(),CCFilesAvailability=round(Count/24*100)) %>% dplyr::filter(Date < today)

RawIndex_CCFiles_Count <- inner_join(RawIndexCount,CCFilesCount,by="Date") %>%
                          dplyr::select_("Date","IndexAvailability","CCFilesAvailability") %>%
                          dplyr::filter(Date < today) %>%
                          dplyr::arrange(desc(Date))

RawIndexCount_xts <- xts(RawIndexCount$IndexAvailability,RawIndexCount$Date)
CCFilesCount_xts <- xts(CCFilesCount$CCFilesAvailability,CCFilesCount$Date)
RawIndex_CCFiles_Count_xts <- cbind(RawIndexCount_xts,CCFilesCount_xts)
colnames(RawIndex_CCFiles_Count_xts)[1] <- "IndexAvailability"
colnames(RawIndex_CCFiles_Count_xts)[2] <- "CCFilesAvailability"

save(RawIndex_CCFiles_Count_xts,
     file="/srv/shiny-server/DataAnalyticsPortal/data/DataQualityMonitoringInformation_TEST.RData")

time_taken <- proc.time() - ptm
ans <- paste("DataQualityMonitoringInformation_TEST successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
