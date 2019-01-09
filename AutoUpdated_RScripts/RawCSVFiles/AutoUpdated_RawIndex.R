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
pacman::p_load(RCurl,stringr,plyr,dplyr,lubridate)

setwd("/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Index")
directory <- "/srv/shiny-server/DataAnalyticsPortal/data/FTP_Server/Index"

Downloaded_IndexFiles <- list.files(path=directory, pattern="RFIC.", full.names=F, recursive=FALSE)
Downloaded_IndexFiles_digits <- str_extract(Downloaded_IndexFiles, "[0-9]+")

ftp_str <- "ftp://SINGAPORE:S1n9ap0r3PF337£@extranet.lyonnaise-des-eaux.fr:8022"
RFIC_files <- getURL(paste0(ftp_str,"/Data/Output/Index_processed/"),ftp.use.epsv = FALSE,dirlistonly = TRUE)
FTP_IndexFiles <- unlist(strsplit(RFIC_files,"\n"))
FTP_IndexFiles <- FTP_IndexFiles[3:length(FTP_IndexFiles)] # because of 1st two are directories

FTP_IndexFiles_digits <- str_extract(FTP_IndexFiles, "[0-9]+") 

# Get the difference between Downloaded and FTP files
FTP_IndexFiles_Diff <- FTP_IndexFiles[!FTP_IndexFiles_digits %in% Downloaded_IndexFiles_digits]

if (length(FTP_IndexFiles_Diff) > 0){
  for (i in 1:length(FTP_IndexFiles_Diff))
  {
    download.file(paste0(ftp_str,"/Data/Output/Index_processed/",FTP_IndexFiles_Diff[i]),paste0("RFIC_",sprintf("%03s", str_extract(FTP_IndexFiles_Diff[i],"[0-9]+")),".CSV.7"),mode = 'wb')
  }
}

# combine files in a single dataframe
options(stringsAsFactors=FALSE)
#require(plyr)
RawIndex <- do.call("rbind.fill",lapply(Downloaded_IndexFiles,
                                              FUN=function(files){
                                                read.csv(files, skip=6, header=F, sep=";")
                                              }
))

colnames(RawIndex) <- c("InternalMeteringPointReference","ExternalMeteringPointReference",
                              "MeteringPointEnergy","ExternalCustomerReference","MeterSerialNumber",
                              "MediaSerialNumber","ReadingDate","ReadingType","ReadingMode","MeasureType01",
                              "Index","Index_Unit","Min_flow_5","Min_flow_5_Value","Min_flow_5_Unit",
                              "Min_flow_15","Min_flow_15_Value","Min_flow_15_Unit",
                              "Max_flow_5","Max_flow_5_Value","Max_flow_5_Unit",
                              "Max_flow_15","Max_flow_15_Value","Max_flow_15_Unit",
                              "BackFlow","BackFlow_Value","BackFlow_Unit" )

RawIndex$ReadingDate <- gsub("T", " ", RawIndex$ReadingDate) # remove T in Consumption_dataset$ReadingDate
RawIndex$ReadingDate <- as.POSIXct(RawIndex$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

RawIndex <- RawIndex[, (colnames(RawIndex) %in% 
                                  c("ExternalMeteringPointReference","MeterSerialNumber",
                                    "ReadingDate","Index","Min_flow_5_Value","Min_flow_15_Value","Max_flow_5_Value",
                                    "Max_flow_5_Value","BackFlow_Value"))] # keep those columns

save(RawIndex, file="/srv/shiny-server/DataAnalyticsPortal/data/RawIndex.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawIndex successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
