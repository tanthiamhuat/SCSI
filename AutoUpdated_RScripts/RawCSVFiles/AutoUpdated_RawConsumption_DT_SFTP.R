# connect to SFTP Server
# hostname : sftp.sitr.suezsmartsolutions.com      
# Username : SINGAPORE
# Password : S1n9ap0r3PF337£
# Port Number: 8122

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RCurl,stringr,plyr,dplyr,lubridate,data.table,fst)

protocol <- "sftp"
server <- "sftp.sitr.suezsmartsolutions.com"
userpwd <- "SINGAPORE:S1n9ap0r3PF337£"

setwd("/srv/shiny-server/DataAnalyticsPortal/data/SFTP/Consumption")
directory <- "/srv/shiny-server/DataAnalyticsPortal/data/SFTP/Consumption"
Downloaded_ConsumptionFiles <- list.files(path=directory, pattern="Consumption_", full.names=F, recursive=FALSE)
Downloaded_ConsumptionFiles_digits <- str_extract(Downloaded_ConsumptionFiles, "[0-9]+")

CC_files <- getURL("sftp://SINGAPORE@sftp.sitr.suezsmartsolutions.com:8122/SINGAPORE/Data/Output/consumption_processed/",
                    userpwd="SINGAPORE:S1n9ap0r3PF337£",ftp.use.epsv = FALSE,dirlistonly = TRUE)

SFTP_ConsumptionFiles <- unlist(strsplit(CC_files,"\n"))

SFTP_ConsumptionFiles_digits <- str_extract(SFTP_ConsumptionFiles, "[0-9]+")

# Get the difference between Downloaded and FTP files
SFTP_ConsumptionFiles_Diff <- SFTP_ConsumptionFiles[!SFTP_ConsumptionFiles_digits %in% Downloaded_ConsumptionFiles_digits]

# https://jonkimanalyze.wordpress.com/2014/11/20/r-quick-sftp-file-transfer/
if (length(SFTP_ConsumptionFiles_Diff) > 2){
  for (i in 3:length(SFTP_ConsumptionFiles_Diff))
  {
    tsfrFilename <- paste0(":8122/SINGAPORE/Data/Output/consumption_processed/",SFTP_ConsumptionFiles_Diff[i])
    outputFilename <- paste0("Consumption_",sprintf("%03s", str_extract(SFTP_ConsumptionFiles_Diff[i],"[0-9]+")),".csv")
    
    ## Download Data
    url <- paste0(protocol, "://", server, tsfrFilename)
    data <- getURL(url = url, userpwd=userpwd)
    
    ## Create File
    fconn <- file(outputFilename)
    writeLines(data, fconn)
    close(fconn)
  }
}

# combine files in a single dataframe
options(stringsAsFactors=FALSE)
Downloaded_ConsumptionFiles <- list.files(path=directory, pattern="Consumption_", full.names=F, recursive=FALSE)

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

RawConsumption <- RawConsumption_GoodData %>% 
                  select(ExternalMeteringPointReference,ExternalCustomerReference,MeterSerialNumber,ReadingDate,Index,Consumption)

RawConsumption$Index <- as.integer(RawConsumption$Index)
RawConsumption$Consumption <- as.integer(RawConsumption$Consumption)

RawConsumption <- as.data.table(RawConsumption)

write.fst(RawConsumption,"/srv/shiny-server/DataAnalyticsPortal/data/DT/RawConsumption.fst",100)
save(RawConsumption, file="/srv/shiny-server/DataAnalyticsPortal/data/DT/RawConsumption.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawConsumption_DT_SFTP successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)