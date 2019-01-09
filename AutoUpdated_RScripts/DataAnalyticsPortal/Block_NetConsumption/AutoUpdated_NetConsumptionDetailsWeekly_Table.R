rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,RPostgreSQL,stringr,ISOweek,lubridate,data.table,tidyr,xts,fst)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption/PunggolWeeklyNetConsumption.RData")

WeeklyConsumption <- PunggolWeeklyNetConsumptionDetails_NA %>%
              dplyr::select_("site","block","meter_sn","meter_type","supply","week","WeeklyConsumption") %>% unique()

WeeklyNetConsumption <- PunggolWeeklyNetConsumptionDetails_NA %>%
  dplyr::group_by(site,block,supply,week) %>%
  dplyr::select_("site","block","supply","week","NetConsumption") %>% unique()

WeeklyNetConsumption$NetConsumption <- as.numeric(WeeklyNetConsumption$NetConsumption)

## remove WeeklyNetConsumption_nonNA from WeeklyNetConsumption

# difference btw 2 dataframe/data.table
setdiffDF <- function(A, B){
  f <- function(A, B)
    A[!duplicated(rbind(B, A))[nrow(B) + 1:nrow(A)], ]
  df1 <- f(A, B)
  df2 <- f(B, A)
  rbind(df1, df2)
}

WeeklyNetConsumption_NA <- WeeklyNetConsumption %>% filter(is.na(NetConsumption))

WeeklyNetConsumption_nonNA_list <- list()
for (j in 1:nrow(WeeklyNetConsumption_NA)){
  WeeklyNetConsumption_nonNA_list[[j]] <- WeeklyNetConsumption %>%
    dplyr::filter(site == WeeklyNetConsumption_NA[j,1] & block == WeeklyNetConsumption_NA[j,2] & 
                  supply == WeeklyNetConsumption_NA[j,3] & week ==  WeeklyNetConsumption_NA[j,4])
}
WeeklyNetConsumption_nonNA <- rbindlist(WeeklyNetConsumption_nonNA_list)
WeeklyNetConsumption_nonNA <- WeeklyNetConsumption_nonNA %>% filter(!is.na(NetConsumption))

WeeklyNetConsumption <- setdiffDF(as.data.frame(WeeklyNetConsumption), WeeklyNetConsumption_nonNA)

WeeklyConsumption_Wide <-  spread(WeeklyConsumption,week,WeeklyConsumption)
WeeklyConsumptionMAINBYPASS_Wide <- WeeklyConsumption_Wide %>% dplyr::filter(meter_type!="SUB")
WeeklyConsumptionSUB_Wide <- WeeklyConsumption_Wide %>% dplyr::filter(meter_type=="SUB")
WeeklyNetConsumption_Wide <-  spread(WeeklyNetConsumption,week,NetConsumption)

WeeklyNetConsumption_Wide$meter_sn <- "Total"
WeeklyNetConsumption_Wide$meter_type <- "NET"

WeeklyConsumptionTable <- rbind(WeeklyConsumption_Wide,WeeklyNetConsumption_Wide)

WeeklyConsumptionTable_SUB <- WeeklyConsumptionTable %>% dplyr::filter(meter_type=="SUB")
WeeklyConsumptionTable_MAIN <- WeeklyConsumptionTable %>% dplyr::filter(meter_type %in% c("MAIN","BYPASS"))

weeknumbers <- names(WeeklyConsumptionTable)[!(names(WeeklyConsumptionTable) %in% c("site","block","meter_sn","meter_type","supply"))]
colnames(WeeklyConsumptionTable)[6:ncol(WeeklyConsumptionTable)] <- weeknumbers

library(plyr)
groupColumns = c("site","block","meter_type","supply")
dataColumns = weeknumbers
WeeklyConsumptionTable_SUB_Sum = ddply(WeeklyConsumptionTable_SUB, groupColumns, function(x) colSums(x[dataColumns]))
WeeklyConsumptionTable_SUB_Sum$meter_sn <- "Total"

groupColumns1 = c("site","block","supply")
WeeklyConsumptionTable_MAIN_Sum = ddply(WeeklyConsumptionTable_MAIN, groupColumns1, function(x) colSums(x[dataColumns]))
WeeklyConsumptionTable_MAIN_Sum$meter_sn <- "Total"
WeeklyConsumptionTable_MAIN_Sum$meter_type <- "MAIN"

WeeklyNetConsumptionDetails <- rbind(rbind(WeeklyConsumptionTable,WeeklyConsumptionTable_SUB_Sum),WeeklyConsumptionTable_MAIN_Sum) %>% unique()

write.csv(WeeklyNetConsumptionDetails,"/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumptionDetails.csv")

Updated_DateTime_NetConsumptionDetails <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(WeeklyNetConsumptionDetails,Updated_DateTime_NetConsumptionDetails,
     file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumptionDetails.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumptionDetailsWeekly_Table successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
