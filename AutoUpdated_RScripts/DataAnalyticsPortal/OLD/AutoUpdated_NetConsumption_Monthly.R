## Monthly Basis: from Oct 2016 onwards

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table,timeDate)

## Reasonable Net Consumption from 2016-08-25 onwards:
##  filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MainMeters consumption stops on 4 October 2016.

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNet_Consumption.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

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

blocks <- c("PG_B1","PG_B2","PG_B3","PG_B4")
# month.start = "2016-12-01"
# month.end = "2016-12-31"

   month.start <- as.character(timeFirstDayInMonth(today()-1))
   month.end <- as.character(timeLastDayInMonth(today()-1))

  consumption_MAIN <- PunggolConsumption_MAIN %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  consumption_SUB <- PunggolConsumption_SUB %>%
                      dplyr::filter(date(date_consumption) >= month.start & date(date_consumption) <= month.end)
  
  for(j in 1: length(blocks)) {
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

    # for PG_B5
    IndirectMainMeters[[5]] <- consumption_MAIN  %>%
                               dplyr::filter(block=="PG_B5" & grepl("M|B",service_point_sn)) %>%
                               dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    DirectMainMeters[[5]] <- consumption_MAIN  %>%
                             dplyr::filter(block=="PG_B5" & !grepl("M|B",service_point_sn)) %>%
                             dplyr::summarise(Total_consumption=sum(interpolated_consumption,na.rm=TRUE))
    TotalMainMeters[[5]] <- IndirectMainMeters[[5]] + DirectMainMeters[[5]]
    IndirectSubMeters[[5]] <- consumption_SUB  %>%
                              dplyr::filter(block=="PG_B5" & room_type!="NIL" & 
                              !(floor=="#01" | floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                              dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    DirectSubMeters[[5]] <- consumption_SUB  %>%
                            dplyr::filter(block=="PG_B5" & room_type!="NIL" & 
                            (floor=="#02" | floor=="#03" | floor=="#04" | floor=="#05")) %>%
                            dplyr::summarise(Total_consumption=sum(adjusted_consumption,na.rm=TRUE))

    IndirectNetConsumption[[5]] <- IndirectMainMeters[[5]]-IndirectSubMeters[[5]]
    DirectNetConsumption[[5]] <- DirectMainMeters[[5]]-DirectSubMeters[[5]]
    TotalNetConsumption[[5]] <- IndirectNetConsumption[[5]]+DirectNetConsumption[[5]]

    TotalNetConsumptionRate[[5]] <- round(TotalNetConsumption[[5]]/TotalMainMeters[[5]]*100,2)

    MonthlyNetConsumption_New <- cbind(Date=as.data.frame(rep(month.end,5)),
                                 Block=as.data.frame(c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")),
                                 IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                                 IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                 DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                                 DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                 TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                                 TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                 TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
    
    MonthlyNetConsumptionCSV_New <- cbind(Date=as.data.frame(rep(month.end,5)),
                                    Block=as.data.frame(c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")),
                                    IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                                    IndirectSubMeters=as.data.frame(unlist(IndirectSubMeters)),
                                    IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                                    DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                                    DirectSubMeters=as.data.frame(unlist(DirectSubMeters)),
                                    DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                                    TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                                    TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                                    TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))

colnames(MonthlyNetConsumption_New) <-c("Date","Block","Indirect\nMainMeters","IndirectNet\nConsumption",
                                        "Direct\nMainMeters","DirectNet\nConsumption","Total\nMainMeters","TotalNet\nConsumption",
                                        "TotalNet\nConsumptionRate")
colnames(MonthlyNetConsumptionCSV_New) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
                                     "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
                                     "TotalNetConsumption","TotalNetConsumptionRate")
    
MonthlyNetConsumption = rbind(MonthlyNetConsumption,MonthlyNetConsumption_New)
MonthlyNetConsumptionCSV = rbind(MonthlyNetConsumptionCSV,MonthlyNetConsumptionCSV_New)

save(MonthlyNetConsumption,MonthlyNetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNet_Consumption.RData")

colnames(MonthlyNetConsumption) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
                              "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
                              "TotalNetConsumptionRate")
# colnames(MonthlyNetConsumptionCSV) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
#                                  "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
#                                  "TotalNetConsumption","TotalNetConsumptionRate")

all_blocks <- c(blocks,"PG_B5")
for (i in 1:length(all_blocks)){
  assign(paste("MonthlyNetConsumption_",all_blocks[i],sep=""), MonthlyNetConsumption %>% filter(Block==all_blocks[i]))
}

MonthlyNetConsumption_PG_B1 <- xts(MonthlyNetConsumption_PG_B1$TotalNetConsumption,order.by=as.Date(MonthlyNetConsumption_PG_B1$Date))
MonthlyNetConsumption_PG_B2 <- xts(MonthlyNetConsumption_PG_B2$TotalNetConsumption,order.by=as.Date(MonthlyNetConsumption_PG_B2$Date))
MonthlyNetConsumption_PG_B3 <- xts(MonthlyNetConsumption_PG_B3$TotalNetConsumption,order.by=as.Date(MonthlyNetConsumption_PG_B3$Date))
MonthlyNetConsumption_PG_B4 <- xts(MonthlyNetConsumption_PG_B4$TotalNetConsumption,order.by=as.Date(MonthlyNetConsumption_PG_B4$Date))
MonthlyNetConsumption_PG_B5 <- xts(MonthlyNetConsumption_PG_B5$TotalNetConsumption,order.by=as.Date(MonthlyNetConsumption_PG_B5$Date))

for (i in 1:length(all_blocks)){
  assign(paste("MonthlyNetConsumptionRate_",all_blocks[i],sep=""), MonthlyNetConsumption %>% filter(Block==all_blocks[i]))
}

MonthlyNetConsumptionRate_PG_B1 <- xts(MonthlyNetConsumptionRate_PG_B1$TotalNetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionRate_PG_B1$Date))
MonthlyNetConsumptionRate_PG_B2 <- xts(MonthlyNetConsumptionRate_PG_B2$TotalNetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionRate_PG_B2$Date))
MonthlyNetConsumptionRate_PG_B3 <- xts(MonthlyNetConsumptionRate_PG_B3$TotalNetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionRate_PG_B3$Date))
MonthlyNetConsumptionRate_PG_B4 <- xts(MonthlyNetConsumptionRate_PG_B4$TotalNetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionRate_PG_B4$Date))
MonthlyNetConsumptionRate_PG_B5 <- xts(MonthlyNetConsumptionRate_PG_B5$TotalNetConsumptionRate,order.by=as.Date(MonthlyNetConsumptionRate_PG_B5$Date))

MonthlyNetConsumption_xts <- cbind(MonthlyNetConsumption_PG_B1,MonthlyNetConsumption_PG_B2,
                                   MonthlyNetConsumption_PG_B3,MonthlyNetConsumption_PG_B4,MonthlyNetConsumption_PG_B5)
colnames(MonthlyNetConsumption_xts)[1:5] <- c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5") 

MonthlyNetConsumptionRate_xts <- cbind(MonthlyNetConsumptionRate_PG_B1,MonthlyNetConsumptionRate_PG_B2,
                                       MonthlyNetConsumptionRate_PG_B3,MonthlyNetConsumptionRate_PG_B4,MonthlyNetConsumptionRate_PG_B5)
colnames(MonthlyNetConsumptionRate_xts)[1:5] <- c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")

colnames(MonthlyNetConsumption) <- c("Date","Block","Indirect\nMainMeters","IndirectNet\nConsumption",
                              "Direct\nMainMeters","DirectNet\nConsumption","Total\nMainMeters","TotalNet\nConsumption",
                              "TotalNet\nConsumptionRate")

MonthlyNetConsumption <- MonthlyNetConsumption %>% dplyr::arrange(desc(Date))
MonthlyNetConsumptionCSV <- MonthlyNetConsumptionCSV %>% dplyr::arrange(desc(Date))

save(MonthlyNetConsumption, MonthlyNetConsumption_xts, MonthlyNetConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.RData")
write.csv(MonthlyNetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/MonthlyNetConsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumption_Monthly successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)