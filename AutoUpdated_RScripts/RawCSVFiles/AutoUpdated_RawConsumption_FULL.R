# connect to FTP Server
# hostname : extranet.lyonnaise-des-eaux.fr      
# Username : SINGAPORE
# Password : S1n9ap0r3PF337£
# Port Number: 8022

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RCurl,stringr,dplyr)

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
require(plyr)
Downloaded_ConsumptionFiles <- list.files(path=directory, pattern="Consumption_", full.names=F, recursive=FALSE)
# 1:2238 of csv files does not have the <STATUS> field, last file is Consumption_160831213000.csv
Downloaded_ConsumptionFiles_before <- Downloaded_ConsumptionFiles[which(Downloaded_ConsumptionFiles_digits < 160901003000)]
RawConsumption_before <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles_before,
                                              FUN=function(files){
                                                read.csv(files, skip=6, header=F, sep=";")
                                              }
))
colnames(RawConsumption_before) <- c("InternalMeteringPointReference","ExternalMeteringPointReference",
                                          "MeteringPointEnergy","ExternalCustomerReference","MeterSerialNumber",
                                          "MediaSerialNumber","ReadingDate","ReadingType","ReadingMode","MeasureType01",
                                          "Index","MeasureUnit01","MeasureType02","Consumption","ConsumptionUnit",
                                          "Nothing1")
RawConsumption_before <- RawConsumption_before[, !(colnames(RawConsumption_before) %in% c("ReadingType","ReadingMode",
                                                                                    "MeasureType01","MeasureUnit01","MeasureType02","Nothing1"))] # remove columns

RawConsumption_before$ReadingDate <- gsub("T", " ", RawConsumption_before$ReadingDate) # remove T in RawConsumption$ReadingDate

RawConsumption_before$ReadingDate <- as.POSIXct(RawConsumption_before$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

RawConsumption_before_GoodData <- RawConsumption_before %>%
                                       group_by(ExternalMeteringPointReference) %>%
                                       arrange(ExternalMeteringPointReference,ReadingDate) %>%
                                       dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
                                       filter(is.na(hr_diff) | hr_diff!=0)

RawConsumption_before <- RawConsumption_before_GoodData %>% select(ExternalMeteringPointReference,ReadingDate,Index,Consumption)
RawConsumption_before <- as.data.frame(RawConsumption_before)

## first file CC_160901003000.csv which additional <STATUS> field comes in.
Downloaded_ConsumptionFiles_after <- Downloaded_ConsumptionFiles[which(Downloaded_ConsumptionFiles_digits >= 160901003000)]
RawConsumption_after <- do.call("rbind.fill",lapply(Downloaded_ConsumptionFiles_after,
                                                          FUN=function(files){
                                                            read.csv(files, skip=6, header=F, sep=";")
                                                          }
))
colnames(RawConsumption_after) <- c("InternalMeteringPointReference","ExternalMeteringPointReference",
                                         "MeteringPointEnergy","ExternalCustomerReference","MeterSerialNumber",
                                          "MediaSerialNumber","ReadingDate","ReadingType","ReadingMode","Status","MeasureType01",
                                          "Index","MeasureUnit01","MeasureType02","Consumption","ConsumptionUnit",
                                          "Nothing1")

RawConsumption_after <- RawConsumption_after[, !(colnames(RawConsumption_after) %in% c("ReadingType","ReadingMode","Status","MeasureType01",
                                                                                                      "MeasureUnit01","MeasureType02","Nothing1"))] # remove columns

RawConsumption_after$ReadingDate <- gsub("T", " ", RawConsumption_after$ReadingDate) # remove T in RawConsumption$ReadingDate

RawConsumption_after$ReadingDate <- as.POSIXct(RawConsumption_after$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

RawConsumption_after_GoodData <- RawConsumption_after %>%
                                      group_by(ExternalMeteringPointReference) %>%
                                      arrange(ExternalMeteringPointReference,ReadingDate) %>%
                                      dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
                                      filter(is.na(hr_diff) | hr_diff!=0)

RawConsumption_after <- RawConsumption_after_GoodData %>% select(ExternalMeteringPointReference,ReadingDate,Index,Consumption)

RawConsumption_after <- as.data.frame(RawConsumption_after)
RawConsumption_FULL <- rbind(RawConsumption_before,RawConsumption_after)

# ConsumptionGoodData <- RawConsumption %>%
#                        group_by(ExternalMeteringPointReference) %>%
#                        arrange(ExternalMeteringPointReference,ReadingDate) %>%
#                        dplyr::mutate(hr_diff = difftime(ReadingDate, lag(ReadingDate),units="hours")) %>%
#                        filter(hr_diff==1)

#RawConsumption <- data.frame(ConsumptionGoodData)
#RawConsumption[length(RawConsumption)] <- NULL  # remove last column

save(RawConsumption_FULL, file="/srv/shiny-server/DataAnalyticsPortal/data/RawConsumption_FULL.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawConsumption_FULL successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)