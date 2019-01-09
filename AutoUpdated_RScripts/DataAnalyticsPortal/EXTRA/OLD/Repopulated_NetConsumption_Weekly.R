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

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
#Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")
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
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- TotalNetConsumptionRate <- list()

all_blocks <- sort(unique(Punggol_All$block))
blocks <- all_blocks[1:length(all_blocks)-1]
lastblock <- all_blocks[length(all_blocks)]

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop
## every Monday
days <- seq(as.Date("2016-09-12"),as.Date("2017-09-25"),by=7)
#days <- seq(as.Date("2017-09-11"),as.Date("2017-09-18"),by=7)
#days <- seq(as.Date("2016-09-12"),as.Date("2017-09-11"),by=7)

WeeklyNetConsumption <- list()
WeeklyNetConsumptionCSV <- list()
for (k in 1:length(days))
{
  weeknumber <- as.numeric(strftime(days[k],format="%W")) 
  i <- weeknumber-1
  if (i <10) {i <- paste(0,i,sep="")}
  WeekNumber=paste(year(days[k]),"_",i,sep="") 
  if (i =="00") {
    i <- 52
    WeekNumber=paste(year(today())-1,"_",i,sep="")
  }
  week.start <- Week.date$beg[which(Week.date$week==WeekNumber)]
  week.end <- Week.date$end[which(Week.date$week==WeekNumber)]
  
  consumption_MAIN <- PunggolConsumption_MAIN %>%
                      dplyr::filter(date(date_consumption) >= week.start & date(date_consumption) <= week.end)
  consumption_SUB <- PunggolConsumption_SUB %>%
                      dplyr::filter(date(date_consumption) >= week.start & date(date_consumption) <= week.end)
  
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
 
    IndirectNetConsumptionRate[[j]] <- round(IndirectNetConsumption[[j]]/IndirectMainMeters[[j]]*100,1)
    DirectNetConsumptionRate[[j]] <- round(DirectNetConsumption[[j]]/DirectMainMeters[[j]]*100,1)
    TotalNetConsumptionRate[[j]] <- round(TotalNetConsumption[[j]]/TotalMainMeters[[j]]*100,1)
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
    
    IndirectNetConsumptionRate[[5]] <- round(IndirectNetConsumption[[5]]/IndirectMainMeters[[5]]*100,1)
    DirectNetConsumptionRate[[5]] <- round(DirectNetConsumption[[5]]/DirectMainMeters[[5]]*100,1)
    TotalNetConsumptionRate[[5]] <- round(TotalNetConsumption[[5]]/TotalMainMeters[[5]]*100,1)
    
    WeeklyNetConsumption[[k]] <- cbind(Date=as.data.frame(rep(week.end,5)),
                                       Block=as.data.frame(all_blocks),
                                       IndirectNetConsumptionRate=as.data.frame(unlist(IndirectNetConsumptionRate)),
                                       DirectNetConsumptionRate=as.data.frame(unlist(DirectNetConsumptionRate)),
                                       TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))

    WeeklyNetConsumptionCSV[[k]] <- cbind(Date=as.data.frame(rep(week.end,5)),
                                          Block=as.data.frame(all_blocks),
                                          IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                                          IndirectSubMeters=as.data.frame(unlist(IndirectSubMeters)),
                                          IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                          IndirectNetConsumptionRate=as.data.frame(unlist(IndirectNetConsumptionRate)),
                                          DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                                          DirectSubMeters=as.data.frame(unlist(DirectSubMeters)),
                                          DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                          DirectNetConsumptionRate=as.data.frame(unlist(DirectNetConsumptionRate)),
                                          TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                                          TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                          TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
}

WeeklyNetConsumption <- as.data.frame(rbindlist(WeeklyNetConsumption))
WeeklyNetConsumptionCSV <- as.data.frame(rbindlist(WeeklyNetConsumptionCSV))
colnames(WeeklyNetConsumption) <- c("Date","Block","IndirectNetConsumptionRate","DirectNetConsumptionRate","TotalNetConsumptionRate")
colnames(WeeklyNetConsumptionCSV) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption","IndirectNetConsumptionRate",
                                       "DirectMainMeters","DirectSubMeters","DirectNetConsumption","DirectNetConsumptionRate",
                                       "TotalMainMeters","TotalNetConsumption","TotalNetConsumptionRate")

save(WeeklyNetConsumption,WeeklyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNet_Consumption.RData")

WeeklyNetConsumptionDate <- WeeklyNetConsumption %>% dplyr::select_("Date") %>% unique()

WeeklyNetConsumptionCSV_TotalNetConsumption <- WeeklyNetConsumptionCSV %>% dplyr::select_("Date","Block","TotalNetConsumption")
WeeklyNetConsumptionCSV_TotalNetConsumption_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumption,Block,TotalNetConsumption)
WeeklyNetConsumptionCSV_TotalNetConsumption_wide["Date"] <- NULL
WeeklyNetConsumption_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumption_wide,order.by=as.Date(WeeklyNetConsumptionDate$Date))

WeeklyNetConsumptionCSV_TotalNetConsumptionRate <- WeeklyNetConsumptionCSV %>% dplyr::select_("Date","Block","TotalNetConsumptionRate")
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide <- spread(WeeklyNetConsumptionCSV_TotalNetConsumptionRate,Block,TotalNetConsumptionRate)
WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide["Date"] <- NULL
WeeklyNetConsumptionRate_xts <- xts(WeeklyNetConsumptionCSV_TotalNetConsumptionRate_wide,order.by=as.Date(WeeklyNetConsumptionDate$Date))

WeeklyNetConsumption <- WeeklyNetConsumption %>% dplyr::arrange(desc(Date))
WeeklyNetConsumptionCSV <- WeeklyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

Updated_DateTime_NetConsumption_Weekly <- paste("Last Updated on",now())

save(WeeklyNetConsumption,WeeklyNetConsumption_xts,WeeklyNetConsumptionRate_xts,Updated_DateTime_NetConsumption_Weekly,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.RData")
write.csv(WeeklyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.csv",row.names=FALSE)
