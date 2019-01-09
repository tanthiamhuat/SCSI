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
pacman::p_load(RCurl,stringr,plyr,dplyr,lubridate,fst)

protocol <- "sftp"
server <- "sftp.sitr.suezsmartsolutions.com"
userpwd <- "SINGAPORE:S1n9ap0r3PF337£"

setwd("/srv/shiny-server/DataAnalyticsPortal/data/SFTP/Index")
directory <- "/srv/shiny-server/DataAnalyticsPortal/data/SFTP/Index"

Downloaded_IndexFiles <- list.files(path=directory, pattern="RFIC.", full.names=F, recursive=FALSE)
Downloaded_IndexFiles_digits <- str_extract(Downloaded_IndexFiles, "[0-9]+")

RFIC_files <- getURL("sftp://SINGAPORE@sftp.sitr.suezsmartsolutions.com:8122/SINGAPORE/Data/Output/index_processed/",
                   userpwd="SINGAPORE:S1n9ap0r3PF337£",ftp.use.epsv = FALSE,dirlistonly = TRUE)
SFTP_IndexFiles <- unlist(strsplit(RFIC_files,"\n"))
SFTP_IndexFiles <- SFTP_IndexFiles[3:length(SFTP_IndexFiles)] # because of 1st two are directories
SFTP_IndexFiles_digits <- str_extract(SFTP_IndexFiles, "[0-9]+") 

# Get the difference between Downloaded and SFTP files
SFTP_IndexFiles_Diff <- SFTP_IndexFiles[!SFTP_IndexFiles_digits %in% Downloaded_IndexFiles_digits]

if (length(SFTP_IndexFiles_Diff) > 0){
  for (i in 1:length(SFTP_IndexFiles_Diff))
  {
    tsfrFilename <- paste0(":8122/SINGAPORE/Data/Output/index_processed/",SFTP_IndexFiles_Diff[i])
    outputFilename <- paste0("RFIC_",sprintf("%03s", str_extract(SFTP_IndexFiles_Diff[i],"[0-9]+")),".csv")
    
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

RawIndex <- RawIndex[, (colnames(RawIndex) %in% 
                                  c("ExternalMeteringPointReference","MeterSerialNumber",
                                    "ReadingDate","Index","Min_flow_5_Value","Min_flow_15_Value","Max_flow_5_Value",
                                    "Max_flow_15_Value","BackFlow_Value"))] # keep those columns

RawIndex$ReadingDate <- gsub("T", " ", RawIndex$ReadingDate) # remove T in Consumption_dataset$ReadingDate
RawIndex$ReadingDate <- as.POSIXct(RawIndex$ReadingDate, format ="%d/%m/%Y %H:%M:%S")

RawIndex_2018 <- RawIndex %>% filter(date(ReadingDate)>="2018-01-01")

write.fst(RawIndex,"/srv/shiny-server/DataAnalyticsPortal/data/DT/RawIndex.fst",100)
save(RawIndex, file="/srv/shiny-server/DataAnalyticsPortal/data/DT/RawIndex.RData")

write.fst(RawIndex_2018,"/srv/shiny-server/DataAnalyticsPortal/data/DT/RawIndex_2018.fst",100)
save(RawIndex_2018, file="/srv/shiny-server/DataAnalyticsPortal/data/DT/RawIndex_2018.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RawIndex_DT_SFTP successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
