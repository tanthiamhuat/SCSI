## Monthly Basis: from Oct 2016 onwards

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,timeDate,fst)

## Reasonable Net Consumption from 2016-08-25 onwards:
##  filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MasterMeters consumption stops on 4 October 2016.

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNet_Consumption.RData")

Punggol_last6months <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

PunggolConsumption_Master <- Punggol_last6months %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS")) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,meter_type,block,interpolated_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

PunggolConsumption_SUB <- Punggol_last6months %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

IndirectNetConsumption <- DirectNetConsumption <- TotalNetConsumption <- list()
IndirectMasterMeters <- DirectMasterMeters <- TotalMasterMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- TotalNetConsumptionRate <- list()

all_blocks <- sort(unique(Punggol_last6months$block))
blocks <- all_blocks[1:length(all_blocks)-1]
last_block <- all_blocks[length(all_blocks)]

# month.start = "2017-10-01"
# month.end = "2017-10-31"

   month.start <- as.character(timeFirstDayInMonth(today()-1))
   month.end <- as.character(timeLastDayInMonth(today()-1))

  consumption_Master <- PunggolConsumption_Master %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  consumption_SUB <- PunggolConsumption_SUB %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  
  for(j in 1: length(blocks)) {
    IndirectMasterMeters[[j]] <- consumption_Master  %>%
                               dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    
    DirectMasterMeters[[j]] <- consumption_Master  %>%
                             dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))

    TotalMasterMeters[[j]] <- IndirectMasterMeters[[j]] + DirectMasterMeters[[j]]
    IndirectSubMeters[[j]] <- consumption_SUB  %>%
                              dplyr::filter(block==blocks[j] & room_type!="NIL" & 
                                     !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))
    DirectSubMeters[[j]] <- consumption_SUB  %>%
                            dplyr::filter(block==blocks[j] & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[j]] <- IndirectMasterMeters[[j]]-IndirectSubMeters[[j]]
    DirectNetConsumption[[j]] <- DirectMasterMeters[[j]]-DirectSubMeters[[j]]
    TotalNetConsumption[[j]] <- IndirectNetConsumption[[j]]+DirectNetConsumption[[j]]
 
    IndirectNetConsumptionRate[[j]] <- round(IndirectNetConsumption[[j]]/IndirectMasterMeters[[j]]*100,1)
    DirectNetConsumptionRate[[j]] <- round(DirectNetConsumption[[j]]/DirectMasterMeters[[j]]*100,1)
    TotalNetConsumptionRate[[j]] <- round(TotalNetConsumption[[j]]/TotalMasterMeters[[j]]*100,1)
  }

    # for PG_B5
    IndirectMasterMeters[[5]] <- consumption_Master  %>%
                               dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    DirectMasterMeters[[5]] <- consumption_Master  %>%
                             dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    TotalMasterMeters[[5]] <- IndirectMasterMeters[[5]] + DirectMasterMeters[[5]]
    IndirectSubMeters[[5]] <- consumption_SUB  %>%
                              dplyr::filter(block==last_block & room_type!="NIL" & 
                              !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    DirectSubMeters[[5]] <- consumption_SUB  %>%
                            dplyr::filter(block==last_block & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[5]] <- IndirectMasterMeters[[5]]-IndirectSubMeters[[5]]
    DirectNetConsumption[[5]] <- DirectMasterMeters[[5]]-DirectSubMeters[[5]]
    TotalNetConsumption[[5]] <- IndirectNetConsumption[[5]]+DirectNetConsumption[[5]]

    IndirectNetConsumptionRate[[5]] <- round(IndirectNetConsumption[[5]]/IndirectMasterMeters[[5]]*100,1)
    DirectNetConsumptionRate[[5]] <- round(DirectNetConsumption[[5]]/DirectMasterMeters[[5]]*100,1)
    TotalNetConsumptionRate[[5]] <- round(TotalNetConsumption[[5]]/TotalMasterMeters[[5]]*100,1)

    MonthlyNetConsumption_New <- cbind(Date=as.data.frame(rep(month.end,5)),
                                       Block=as.data.frame(all_blocks),
                                       IndirectNetConsumptionRate=as.data.frame(unlist(IndirectNetConsumptionRate)),
                                       DirectNetConsumptionRate=as.data.frame(unlist(DirectNetConsumptionRate)),
                                       TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
    
    MonthlyNetConsumptionCSV_New <- cbind(Date=as.data.frame(rep(month.end,5)),
                                          Block=as.data.frame(all_blocks),
                                          IndirectMasterMeters=as.data.frame(unlist(IndirectMasterMeters)),
                                          IndirectSubMeters=as.data.frame(unlist(IndirectSubMeters)),
                                          IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                          IndirectNetConsumptionRate=as.data.frame(unlist(IndirectNetConsumptionRate)),
                                          DirectMasterMeters=as.data.frame(unlist(DirectMasterMeters)),
                                          DirectSubMeters=as.data.frame(unlist(DirectSubMeters)),
                                          DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                          DirectNetConsumptionRate=as.data.frame(unlist(DirectNetConsumptionRate)),
                                          TotalMasterMeters=as.data.frame(unlist(TotalMasterMeters)),
                                          TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                          TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))

colnames(MonthlyNetConsumption_New) <-c("Date","Block","IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")
colnames(MonthlyNetConsumptionCSV_New) <- c("Date","Block","IndirectMasterMeters","IndirectSubMeters","IndirectNetConsumption","IndirectNetConsumptionRate",
                                            "DirectMasterMeters","DirectSubMeters","DirectNetConsumption","DirectNetConsumptionRate",
                                            "TotalMasterMeters","TotalNetConsumption","TotalNetConsumptionRate")
    
MonthlyNetConsumption = rbind(MonthlyNetConsumption,MonthlyNetConsumption_New)
MonthlyNetConsumptionCSV = rbind(MonthlyNetConsumptionCSV,MonthlyNetConsumptionCSV_New)

save(MonthlyNetConsumption,MonthlyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNet_Consumption.RData")

MonthlyNetConsumptionDate <- MonthlyNetConsumption %>% dplyr::select_("Date") %>% unique()

MonthlyNetConsumptionCSV_TotalNetConsumption <- MonthlyNetConsumptionCSV %>% dplyr::select_("Date","Block","TotalNetConsumption")
MonthlyNetConsumptionCSV_TotalNetConsumption_wide <- spread(MonthlyNetConsumptionCSV_TotalNetConsumption,Block,TotalNetConsumption)
MonthlyNetConsumptionCSV_TotalNetConsumption_wide["Date"] <- NULL
MonthlyNetConsumption_xts <- xts(MonthlyNetConsumptionCSV_TotalNetConsumption_wide,order.by=as.Date(MonthlyNetConsumptionDate$Date))

MonthlyNetConsumptionCSV_TotalNetConsumptionRate <- MonthlyNetConsumptionCSV %>% dplyr::select_("Date","Block","TotalNetConsumptionRate")
MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide <- spread(MonthlyNetConsumptionCSV_TotalNetConsumptionRate,Block,TotalNetConsumptionRate)
MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide["Date"] <- NULL
MonthlyNetConsumptionRate_xts <- xts(MonthlyNetConsumptionCSV_TotalNetConsumptionRate_wide,order.by=as.Date(MonthlyNetConsumptionDate$Date))

colnames(MonthlyNetConsumption) <- c("Date","Block","IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")

MonthlyNetConsumption <- MonthlyNetConsumption %>% dplyr::arrange(desc(Date))
MonthlyNetConsumptionCSV <- MonthlyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

Updated_DateTime_NetConsumption_Monthly <- paste("Last Updated on ",now(),"."," Next Update on ",now()+months(1),".",sep="")

save(MonthlyNetConsumption,MonthlyNetConsumption_xts,MonthlyNetConsumptionRate_xts,Updated_DateTime_NetConsumption_Monthly,file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.RData")
write.csv(MonthlyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumption_Monthly successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)