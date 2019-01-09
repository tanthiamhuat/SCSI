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
## filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MainMeters consumption stops on 4 October 2016.

Punggol_last6months <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last6months.fst")

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

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyNet_Consumption.RData")

IndirectNetConsumption <- DirectNetConsumption <- TotalNetConsumption <- list()
IndirectMainMeters <- DirectMainMeters <- TotalMainMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- list()
TotalNetConsumptionRate <- list()

all_blocks <- sort(unique(Punggol_All$block))
blocks <- all_blocks[1:length(all_blocks)-1]
last_block <- all_blocks[length(all_blocks)]

# http://stackoverflow.com/questions/29402528/append-data-frames-together-in-a-for-loop

  consumption_MAIN <- PunggolConsumption_MAIN %>%
                      dplyr::filter(date(date_consumption) >= "2016-08-25")
  consumption_SUB <- PunggolConsumption_SUB %>%
                      dplyr::filter(date(date_consumption) >= "2016-08-25")
  
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

    DailyNetConsumption_New <- cbind(Date=as.data.frame(rep(week.end,5)),
                            Block=as.data.frame(all_blocks),
                            IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                            IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                            DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                            DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                            TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                            TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                            TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
    
    DailyNetConsumptionCSV_New <- cbind(Date=as.data.frame(rep(week.end,5)),
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

colnames(DailyNetConsumption_New) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
                              "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
                              "TotalNetConsumptionRate")
colnames(DailyNetConsumptionCSV_New) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
                                     "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
                                     "TotalNetConsumption","TotalNetConsumptionRate")
    
DailyNetConsumption = rbind(DailyNetConsumption,DailyNetConsumption_New)
DailyNetConsumptionCSV = rbind(DailyNetConsumptionCSV,DailyNetConsumptionCSV_New)

save(DailyNetConsumption,DailyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/DailyNet_Consumption.RData")

DailyNetConsumptionDate <- DailyNetConsumption %>% dplyr::select_("Date") %>% unique()

DailyNetConsumptionList <- list()
for(i in 1:length(all_blocks)){
  DailyNetConsumptionList[[i]] <- DailyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumption")
}

Daily_NetConsumption <- as.data.frame(unlist(DailyNetConsumptionList))
nr <- nrow(Daily_NetConsumption)
n <- nr/length(all_blocks)
Daily_NetConsumption <- as.data.frame(split(Daily_NetConsumption, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Daily_NetConsumption) <- all_blocks
DailyNetConsumption_xts <- xts(Daily_NetConsumption,order.by=as.Date(DailyNetConsumptionDate$Date))

DailyNetConsumptionRateList <- list()
for(i in 1:length(all_blocks)){
  DailyNetConsumptionRateList[[i]] <- DailyNetConsumption %>% filter(Block==all_blocks[i]) %>% dplyr::select_("TotalNetConsumptionRate")
}

Daily_NetConsumptionRate <- as.data.frame(unlist(DailyNetConsumptionRateList))
nr <- nrow(Daily_NetConsumptionRate)
n <- nr/length(all_blocks)
Daily_NetConsumptionRate <- as.data.frame(split(Daily_NetConsumptionRate, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(Daily_NetConsumptionRate) <- all_blocks
DailyNetConsumptionRate_xts <- xts(Daily_NetConsumptionRate,order.by=as.Date(DailyNetConsumptionDate$Date))

colnames(DailyNetConsumption) <- c("Date","Block","Indirect\nMainMeters","IndirectNet\nConsumption",
                              "Direct\nMainMeters","DirectNet\nConsumption","Total\nMainMeters","TotalNet\nConsumption",
                              "TotalNet\nConsumptionRate")

DailyNetConsumption <- DailyNetConsumption %>% dplyr::arrange(desc(Date))
DailyNetConsumptionCSV <- DailyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

save(DailyNetConsumption, DailyNetConsumption_xts, DailyNetConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/DailyNetConsumption.RData")
write.csv(DailyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/DailyNetConsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumption_Daily successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
