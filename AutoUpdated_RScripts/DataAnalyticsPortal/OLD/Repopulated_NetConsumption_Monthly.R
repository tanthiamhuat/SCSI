## Total Net Consumption to be in m^3

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,tidyr,fst)

## Reasonable Net Consumption from 2016-08-25 onwards:
##  filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MainMeters consumption stops on 4 October 2016.

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

PunggolConsumption_MAIN <- Punggol_All %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS")) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,meter_type,block,interpolated_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

IndirectNetConsumption <- DirectNetConsumption <- TotalNetConsumption <- list()
IndirectMainMeters <- DirectMainMeters <- TotalMainMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- list()
TotalNetConsumptionRate <- list()

all_blocks <- sort(unique(Punggol_All$block))
blocks <- all_blocks[1:length(all_blocks)-1]
lastblock <- all_blocks[length(all_blocks)]

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop
months <- seq(as.Date("2016-10-01"),as.Date("2017-09-01"),by=31)

MonthlyNetConsumption <- list()
MonthlyNetConsumptionCSV <- list()
for (k in 1:length(months))
{
  month.start <- as.character(timeFirstDayInMonth(months[k]))
  month.end <- as.character(timeLastDayInMonth(months[k]))
  
  consumption_MAIN <- PunggolConsumption_MAIN %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  consumption_SUB <- PunggolConsumption_SUB %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  
  for(j in 1:length(blocks)) {
    IndirectMainMeters[[j]] <- consumption_MAIN  %>%
                               dplyr::filter(block==blocks[j] & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    
    DirectMainMeters[[j]] <- consumption_MAIN  %>%
                             dplyr::filter(block==blocks[j] & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))

    TotalMainMeters[[j]] <- IndirectMainMeters[[j]] + DirectMainMeters[[j]]
    IndirectSubMeters[[j]] <- consumption_SUB  %>%
                              dplyr::filter(block==blocks[j] & room_type!="NIL" & 
                                     !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))
    DirectSubMeters[[j]] <- consumption_SUB  %>%
                            dplyr::filter(block==blocks[j] & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[j]] <- IndirectMainMeters[[j]]-IndirectSubMeters[[j]]
    DirectNetConsumption[[j]] <- DirectMainMeters[[j]]-DirectSubMeters[[j]]
    TotalNetConsumption[[j]] <- IndirectNetConsumption[[j]]+DirectNetConsumption[[j]]
 
    TotalNetConsumptionRate[[j]] <- round(TotalNetConsumption[[j]]/TotalMainMeters[[j]]*100,2)
  }

    # for PG_B5, lastblock
    IndirectMainMeters[[5]] <- consumption_MAIN  %>%
                               dplyr::filter(block==lastblock & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    DirectMainMeters[[5]] <- consumption_MAIN  %>%
                             dplyr::filter(block==lastblock & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    TotalMainMeters[[5]] <- IndirectMainMeters[[5]] + DirectMainMeters[[5]]
    IndirectSubMeters[[5]] <- consumption_SUB  %>%
                              dplyr::filter(block==lastblock & room_type!="NIL" & 
                              !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    DirectSubMeters[[5]] <- consumption_SUB  %>%
                            dplyr::filter(block==lastblock & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[5]] <- IndirectMainMeters[[5]]-IndirectSubMeters[[5]]
    DirectNetConsumption[[5]] <- DirectMainMeters[[5]]-DirectSubMeters[[5]]
    TotalNetConsumption[[5]] <- IndirectNetConsumption[[5]]+DirectNetConsumption[[5]]

    TotalNetConsumptionRate[[5]] <- round(TotalNetConsumption[[5]]/TotalMainMeters[[5]]*100,2)
    
    MonthlyNetConsumption[[k]] <- cbind(Date=as.data.frame(rep(month.end,5)),
                                       Block=as.data.frame(all_blocks),
                                       IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                                       IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                       DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                                       DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                       TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                                       TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                       TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))

    MonthlyNetConsumptionCSV[[k]] <- cbind(Date=as.data.frame(rep(month.end,5)),
                                          Block=as.data.frame(all_blocks),
                                          IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                                          IndirectSubMeters=as.data.frame(unlist(IndirectSubMeters)),
                                          IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                          DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                                          DirectSubMeters=as.data.frame(unlist(DirectSubMeters)),
                                          DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                          TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                                          TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                          TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
}
MonthlyNetConsumption <- as.data.frame(rbindlist(MonthlyNetConsumption))
MonthlyNetConsumptionCSV <- as.data.frame(rbindlist(MonthlyNetConsumptionCSV))
colnames(MonthlyNetConsumption) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
                                    "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
                                    "TotalNetConsumptionRate")
colnames(MonthlyNetConsumptionCSV) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
                                       "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
                                       "TotalNetConsumption","TotalNetConsumptionRate")

save(MonthlyNetConsumption,MonthlyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNet_Consumption.RData")

MonthlyNetConsumptionDate <- MonthlyNetConsumption %>% dplyr::select_("Date") %>% unique()

MonthlyNetConsumptionList <- list()
for(i in 1:length(all_blocks)){
  MonthlyNetConsumptionList[[i]] <- MonthlyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumption")
}

Monthly_NetConsumption <- as.data.frame(unlist(MonthlyNetConsumptionList))
nr <- nrow(Monthly_NetConsumption)
n <- nr/length(all_blocks)
Monthly_NetConsumption <- as.data.frame(split(Monthly_NetConsumption, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Monthly_NetConsumption) <- all_blocks
MonthlyNetConsumption_xts <- xts(Monthly_NetConsumption,order.by=as.Date(MonthlyNetConsumptionDate$Date))

MonthlyNetConsumptionRateList <- list()
for(i in 1:length(all_blocks)){
  MonthlyNetConsumptionRateList[[i]] <- MonthlyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumptionRate")
}

Monthly_NetConsumptionRate <- as.data.frame(unlist(MonthlyNetConsumptionRateList))
nr <- nrow(Monthly_NetConsumptionRate)
n <- nr/length(all_blocks)
Monthly_NetConsumptionRate <- as.data.frame(split(Monthly_NetConsumptionRate, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Monthly_NetConsumptionRate) <- all_blocks
MonthlyNetConsumptionRate_xts <- xts(Monthly_NetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionDate$Date))


MonthlyNetConsumption <- MonthlyNetConsumption %>% dplyr::arrange(desc(Date))
MonthlyNetConsumptionCSV <- MonthlyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

save(MonthlyNetConsumption, MonthlyNetConsumption_xts, MonthlyNetConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.RData")
write.csv(MonthlyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.csv",row.names=FALSE)
