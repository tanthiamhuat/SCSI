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
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,gdata,tidyr,fst)

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))  
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

consumption_thisyear_servicepoint <- read.fst(paste0(path,'data/DT/consumption_thisyear_servicepoint.fst'),as.data.table = TRUE)
PunggolYuhua_SUB <- consumption_thisyear_servicepoint[site %in% c("Punggol","Yuhua") & meter_type=="SUB" & adjusted_consumption != "NA" & date_consumption >="2018-04-01" &
                                                      !block %in% c("YH_B1","YH_B4","YH_B6")] 
## reliable date for YH starts in April 2018

PunggolYuhua_SUB_Freshness <- PunggolYuhua_SUB %>% dplyr::group_by(service_point_sn,block) %>%
                              dplyr::summarise(LastestTime=max(date_consumption)) %>%
                              dplyr::mutate(Freshness=round(abs(difftime(LastestTime,now(),units = "hours")),2)) %>% 
                              dplyr::filter(date(LastestTime)==today())
PunggolYuhua_SUB_Freshness$Freshness <- as.numeric(PunggolYuhua_SUB_Freshness$Freshness)

Updated_DateTime_DataFreshness <- paste("Last Updated on ",now(),"."," Next Update on ",now()+60*60,".",sep="")

save(PunggolYuhua_SUB_Freshness,Updated_DateTime_DataFreshness,file=paste0(path,'data/PunggolYuhua_SUB_Freshness.RData'))

time_taken <- proc.time() - ptm
ans <- paste("DataFreshness successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)

# p <- plot_ly(x = h$mids, y = h$counts) %>% 
#   add_bars() %>% 
#   layout(title = "Histogram for Customers' Freshness",
#          xaxis = list(title = "Freshness (hrs)"),
#          yaxis = list (title = "Number of Customers")) 
# p