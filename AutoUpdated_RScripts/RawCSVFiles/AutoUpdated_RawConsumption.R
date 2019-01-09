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

setwd("/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Consumption")
directory <- "/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Consumption"
Downloaded_ConsumptionFiles <- list.files(path=directory, pattern="Consumption_", full.names=F, recursive=FALSE)
Downloaded_ConsumptionFiles_digits <- str_extract(Downloaded_ConsumptionFiles, "[0-9]+")

ftp_str <- "ftp://SINGAPORE:S1n9ap0r3PF337£@extranet.lyonnaise-des-eaux.fr:8022"
CC_files <- getURL(paste0(ftp_str,"/Data/Output/Consumption_processed/"),ftp.use.epsv = FALSE,dirlistonly = TRUE)
FTP_ConsumptionFiles <- unlist(strsplit(CC_files,"\n"))

FTP_ConsumptionFiles_digits <- str_extract(FTP_ConsumptionFiles, "[0-9]+")

# Get the difference between Downloaded and FTP files
FTP_ConsumptionFiles_Diff <- FTP_ConsumptionFiles[!FTP_ConsumptionFiles_digits %in% Downloaded_ConsumptionFiles_digits]

if (length(FTP_ConsumptionFiles_Diff) > 0){
  for (i in 1:length(FTP_ConsumptionFiles_Diff))
  {
    download.file(paste0(ftp_str,"/Data/Output/Consumption_processed/",FTP_ConsumptionFiles_Diff[i]),paste0("Consumption_",sprintf("%03s", str_extract(FTP_ConsumptionFiles_Diff[i],"[0-9]+")),".csv"),mode = 'wb')
  }
}

# combine files in a single dataframe
options(stringsAsFactors=FALSE)
#require(plyr)
Downloaded_ConsumptionFiles <- list.files(path=directory, pattern="Consumption_", full.names=F, recursive=FALSE)
# 1:2238 of csv files does not have the <STATUS> field, last file is Consumption_160831213000.csv
# Downloaded_ConsumptionFiles_before <- Downloaded_ConsumptionFiles[which(Downloaded_ConsumptionFiles_digits < 160901003000)]

## first file CC_160901003000.csv which additional <STATUS> field comes in.
#Downloaded_ConsumptionFiles_after <- Downloaded_ConsumptionFiles[which(Downloaded_ConsumptionFiles_digits >= 160901003000)]
# RawConsumption <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles_after,
#                                                           FUN=function(files){
#                                                             read.csv(files, skip=6, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
#                                                           }
# ))

RawConsumption <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles,
                                              FUN=function(files){
                                                read.csv(files, skip=6, header=F, sep=";",colClasses='character') # colClasses='character' to inclde leading zeros
                                              }
))

colnames(RawConsumption) <- c("InternalMeteringPointReference","ExternalMeteringPointReference",
                                         "MeteringPointEnergy","ExternalCustomerReference","MeterSerialNumber",
                                          "MediaSerialNumber","ReadingDate","ReadingType","ReadingMode","Status","MeasureType01",
                                          "Index","MeasureUnit01","MeasureType02","Consumption","ConsumptionUnit",
                                          "Nothing1")

RawConsumption <- RawConsumption[, !(colnames(RawConsumption) %in% c("ReadingType","ReadingMode","Status","MeasureType01",
                                                                      "MeasureUnit01","MeasureType02","Nothing1"))] # remove columns

RawConsumption$ReadingDate <- gsub("T", " ", RawConsumption$ReadingDate) # remove T in RawConsumption$ReadingDate

RawConsumption$ReadingDate <- as.POSIXct(RawConsumption$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

RawConsumption_GoodData <- RawConsumption %>%
                           group_by(ExternalMeteringPointReference) %>%
                           arrange(ExternalMeteringPointReference,ReadingDate) 
                           # %>%
                           # dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
                           # filter(is.na(hr_diff) | hr_diff!=0)

RawConsumption <- RawConsumption_GoodData %>% 
                  select(ExternalMeteringPointReference,ExternalCustomerReference,MeterSerialNumber,ReadingDate,Index,Consumption)

RawConsumption$Index <- as.integer(RawConsumption$Index)
RawConsumption$Consumption <- as.integer(RawConsumption$Consumption)

RawConsumption <- as.data.frame(RawConsumption)

RawConsumption_2017 <- RawConsumption %>% dplyr::filter(date(ReadingDate)>="2017-01-01" & date(ReadingDate)<="2017-12-31")

save(RawConsumption, file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption.RData")
save(RawConsumption_2017, file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_2017.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)