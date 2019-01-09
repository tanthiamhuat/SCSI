## Total Net Consumption to be in m^3

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,fst)

## Reasonable Net Consumption from 2016-08-25 onwards:
##  filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MainMeters consumption stops on 4 October 2016.

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
# Punggol_All <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.fst")
# Punggol_All$date_consumption <- as.POSIXct(Punggol_All$date_consumption, origin="1970-01-01")
# Punggol_All$adjusted_date <- as.POSIXct(Punggol_All$adjusted_date, origin="1970-01-01")
# Punggol_All$Date.Time <- as.POSIXct(Punggol_All$Date.Time, origin="1970-01-01")

Punggol_last6months <- fstread("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")
Punggol_last6months$date_consumption <- as.POSIXct(Punggol_last6months$date_consumption, origin="1970-01-01")
Punggol_last6months$adjusted_date <- as.POSIXct(Punggol_last6months$adjusted_date, origin="1970-01-01")
Punggol_last6months$Date.Time <- as.POSIXct(Punggol_last6months$Date.Time, origin="1970-01-01")

PunggolConsumption_MAIN <- Punggol_last6months %>%
  dplyr::filter((meter_type=="MAIN" |meter_type=="BYPASS")) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,meter_type,block,interpolated_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

PunggolConsumption_SUB <- Punggol_last6months %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

load("/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNet_Consumption.RData")

IndirectNetConsumption <- DirectNetConsumption <- TotalNetConsumption <- list()
IndirectMainMeters <- DirectMainMeters <- TotalMainMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- list()
TotalNetConsumptionRate <- list()

#all_blocks <- sort(unique(Punggol_All$block))
all_blocks <- sort(unique(Punggol_last6months$block))
blocks <- all_blocks[1:length(all_blocks)-1]
last_block <- all_blocks[length(all_blocks)]

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop
weeknumber <- as.numeric(strftime(today(),format="%W")) 
  i <- weeknumber-1
  if (i <10) {i <- paste(0,i,sep="")}
  WeekNumber=paste(year(today()),"_",i,sep="") 
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
 
    TotalNetConsumptionRate[[j]] <- round(TotalNetConsumption[[j]]/TotalMainMeters[[j]]*100,2)
  }

    # for PG_B5, last_block
    IndirectMainMeters[[5]] <- consumption_MAIN  %>%
                               dplyr::filter(block==last_block & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    DirectMainMeters[[5]] <- consumption_MAIN  %>%
                             dplyr::filter(block==last_block & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    TotalMainMeters[[5]] <- IndirectMainMeters[[5]] + DirectMainMeters[[5]]
    IndirectSubMeters[[5]] <- consumption_SUB  %>%
                              dplyr::filter(block==last_block & room_type!="NIL" & 
                              !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    DirectSubMeters[[5]] <- consumption_SUB  %>%
                            dplyr::filter(block==last_block & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[5]] <- IndirectMainMeters[[5]]-IndirectSubMeters[[5]]
    DirectNetConsumption[[5]] <- DirectMainMeters[[5]]-DirectSubMeters[[5]]
    TotalNetConsumption[[5]] <- IndirectNetConsumption[[5]]+DirectNetConsumption[[5]]

    TotalNetConsumptionRate[[5]] <- round(TotalNetConsumption[[5]]/TotalMainMeters[[5]]*100,2)

    WeeklyNetConsumption_New <- cbind(Date=as.data.frame(rep(week.end,5)),
                            Block=as.data.frame(all_blocks),
                            IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                            IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                            DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                            DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                            TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                            TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                            TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
    
    WeeklyNetConsumptionCSV_New <- cbind(Date=as.data.frame(rep(week.end,5)),
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

colnames(WeeklyNetConsumption_New) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
                              "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
                              "TotalNetConsumptionRate")
colnames(WeeklyNetConsumptionCSV_New) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
                                     "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
                                     "TotalNetConsumption","TotalNetConsumptionRate")
    
WeeklyNetConsumption = rbind(WeeklyNetConsumption,WeeklyNetConsumption_New)
WeeklyNetConsumptionCSV = rbind(WeeklyNetConsumptionCSV,WeeklyNetConsumptionCSV_New)

save(WeeklyNetConsumption,WeeklyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNet_Consumption.RData")

WeeklyNetConsumptionDate <- WeeklyNetConsumption %>% dplyr::select_("Date") %>% unique()

WeeklyNetConsumptionList <- list()
for(i in 1:length(all_blocks)){
  WeeklyNetConsumptionList[[i]] <- WeeklyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumption")
}

Weekly_NetConsumption <- as.data.frame(unlist(WeeklyNetConsumptionList))
nr <- nrow(Weekly_NetConsumption)
n <- nr/length(all_blocks)
Weekly_NetConsumption <- as.data.frame(split(Weekly_NetConsumption, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Weekly_NetConsumption) <- all_blocks
WeeklyNetConsumption_xts <- xts(Weekly_NetConsumption,order.by=as.Date(WeeklyNetConsumptionDate$Date))

WeeklyNetConsumptionRateList <- list()
for(i in 1:length(all_blocks)){
  WeeklyNetConsumptionRateList[[i]] <- WeeklyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumptionRate")
}

Weekly_NetConsumptionRate <- as.data.frame(unlist(WeeklyNetConsumptionRateList))
nr <- nrow(Weekly_NetConsumptionRate)
n <- nr/length(all_blocks)
Weekly_NetConsumptionRate <- as.data.frame(split(Weekly_NetConsumptionRate, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Weekly_NetConsumptionRate) <- all_blocks
WeeklyNetConsumptionRate_xts <- xts(Weekly_NetConsumptionRate,order.by=as.Date(WeeklyNetConsumptionDate$Date))

colnames(WeeklyNetConsumption) <- c("Date","Block","Indirect\nMainMeters","IndirectNet\nConsumption",
                              "Direct\nMainMeters","DirectNet\nConsumption","Total\nMainMeters","TotalNet\nConsumption",
                              "TotalNet\nConsumptionRate")

WeeklyNetConsumption <- WeeklyNetConsumption %>% dplyr::arrange(desc(Date))
WeeklyNetConsumptionCSV <- WeeklyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

Updated_DateTime_NetConsumption <- paste("Last Updated on",now())

save(WeeklyNetConsumption,WeeklyNetConsumption_xts,WeeklyNetConsumptionRate_xts,Updated_DateTime_NetConsumption,file="/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.RData")
write.csv(WeeklyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/WeeklyNetConsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumption_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
