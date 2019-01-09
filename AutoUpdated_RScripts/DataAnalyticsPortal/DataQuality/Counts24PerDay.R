rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}

require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst,tidyr)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

consumption_last90days_servicepoint <- read.fst(paste0(path,'data/DT/consumption_last90days_servicepoint.fst'),as.data.table=T)
consumption_last90days_servicepoint <- consumption_last90days_servicepoint %>% dplyr::filter(service_point_sn !="3100660792") %>%
                                       dplyr::mutate(block=ifelse(is.na(block),"TUAS",block))
yesterday <- today()-1
Consumption_Count <- consumption_last90days_servicepoint %>% dplyr::filter(service_point_sn !="3100660792") %>%   ## exclude AHL, which is put under YH_B1
                     dplyr::mutate(Date=date(date_consumption)) %>%
                     dplyr::group_by(site,block,Date,service_point_sn) %>% 
                     dplyr::filter(Date>= "2018-04-01" & Date <= yesterday) %>%
                     dplyr::summarise(Count=n())

Consumption_Count_24 <- Consumption_Count %>% dplyr::filter(Count==24)
days_count <- as.numeric(max(Consumption_Count_24$Date)-min(Consumption_Count_24$Date))

Consumption_Count_24_Block <- Consumption_Count_24 %>%
                              dplyr::group_by(block,Date) %>%
                              dplyr::summarise(BlockCount=n()) 

Counts_PerBlock <- Consumption_Count_24_Block %>% dplyr::group_by(block) %>% dplyr::summarise(CountBlock=n())
Counts_PerBlock_PG <- Counts_PerBlock %>% dplyr::filter(substr(block,1,2)=="PG")
Counts_PerBlock_Tuas <- Counts_PerBlock %>% dplyr::filter(block=="TUAS")
Counts_PerBlock_YH <- Counts_PerBlock %>% dplyr::filter(substr(block,1,2)=="YH")

servicepoint_blocks <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(status=="ACTIVE") %>%
  dplyr::mutate(block=substr(address,1,5)) %>%
  dplyr::filter(!block %in% c("FAKE ","PUNGG")) %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(BlockCount=n()) %>%  # need to add 3 to Punggol blocks for MAIN/BYPASS
  dplyr::mutate(BlockCount=ifelse(substr(block,1,2)=="PG",BlockCount+3,BlockCount))

servicepoint_PG <- servicepoint_blocks %>% dplyr::filter(substr(block,1,2)=="PG")
servicepoint_YH <- servicepoint_blocks %>% dplyr::filter(substr(block,1,2)=="YH")
servicepoint_Tuas <- servicepoint_blocks %>% dplyr::filter(substr(block,1,4)=="TUAS")

Consumption_Count_24_Block$ExpectedCount <- c(rep(servicepoint_PG$BlockCount,c(Counts_PerBlock_PG$CountBlock)),
                                              rep(servicepoint_Tuas$BlockCount,c(Counts_PerBlock_Tuas$CountBlock)),
                                              rep(servicepoint_YH$BlockCount,c(Counts_PerBlock_YH$CountBlock)))

Consumption_Count_24Block <- Consumption_Count_24_Block %>% 
                              dplyr::mutate(Count24Percent=round(BlockCount/ExpectedCount*100)) 
Consumption_Count_24Block["BlockCount"] <- NULL
Consumption_Count_24Block["ExpectedCount"] <- NULL

Consumption_Count_24Block_wide <- tidyr::spread(Consumption_Count_24Block,block,Count24Percent)

Consumption_Count_24Block_wide <- Consumption_Count_24Block_wide %>% dplyr::arrange(desc(Date))

Updated_DateTime_Counts24PerDay <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(Consumption_Count_24Block_wide,Updated_DateTime_Counts24PerDay,
     file=paste0(path,'data/Consumption_Count_24_Block_wide.RData'))

time_taken <- proc.time() - ptm
ans <- paste("Counts24PerDay successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)

## YH_B7 additional customer 3000726064 id_service_point=1168 ??