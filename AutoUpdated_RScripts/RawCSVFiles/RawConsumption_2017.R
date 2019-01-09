# connect to FTP Server
# hostname : extranet.lyonnaise-des-eaux.fr      
# Username : SINGAPORE
# Password : S1n9ap0r3PF337£
# Port Number: 8022

# rm(list=ls())  # remove all variables
# cat("\014")    # clear Console
# if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RCurl,stringr,plyr,dplyr,lubridate)

directory <- "/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Consumption_2017"

# combine files in a single dataframe
options(stringsAsFactors=FALSE)
#require(plyr)

RawConsumption <- list()
months <- c("jan")

for (i in 1:length(months)){
Downloaded_ConsumptionFiles <- list.files(path=paste(directory,"/",months[i],sep=""), pattern="CC_", full.names=F, recursive=FALSE)
setwd(path)
RawConsumption[[i]] <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles,
                                                          FUN=function(files){
                                                            read.csv(files, skip=6, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
                                                          }
                     ))
}
RawConsumption_Test <- as.data.frame(RawConsumption)

colnames(RawConsumption) <- c("InternalMeteringPointReference","ExternalMeteringPointReference",
                                         "MeteringPointEnergy","ExternalCustomerReference","MeterSerialNumber",
                                          "MediaSerialNumber","ReadingDate","ReadingType","ReadingMode","Status","MeasureType01",
                                          "Index","MeasureUnit01","MeasureType02","Consumption","ConsumptionUnit",
                                          "Nothing1")

RawConsumption <- RawConsumption[, !(colnames(RawConsumption) %in% c("ReadingType","ReadingMode","Status","MeasureType01",
                                                                      "MeasureUnit01","MeasureType02","Nothing1"))] # remove columns

RawConsumption$ReadingDate <- gsub("T", " ", RawConsumption$ReadingDate) # remove T in RawConsumption$ReadingDate

RawConsumption$ReadingDate <- as.POSIXct(RawConsumption$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

# RawConsumption_GoodData <- RawConsumption %>%
#                            group_by(ExternalMeteringPointReference) %>%
#                            arrange(ExternalMeteringPointReference,ReadingDate) %>%
#                            dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
#                            filter(is.na(hr_diff) | hr_diff!=0)

RawConsumption <- RawConsumption %>% 
                  select(ExternalMeteringPointReference,ExternalCustomerReference,MeterSerialNumber,ReadingDate,Index,Consumption)

RawConsumption$Index <- as.integer(RawConsumption$Index)
RawConsumption$Consumption <- as.integer(RawConsumption$Consumption)

RawConsumption <- as.data.frame(RawConsumption)

RawConsumption_2017 <- RawConsumption %>% dplyr::filter(date(ReadingDate)>="2017-01-01" & date(ReadingDate)<="2017-12-31")

save(RawConsumption_2017, file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_2017.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)