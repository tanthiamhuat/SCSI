rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,xts,data.table)

## Reasonable Net Consumption from 2016-08-25 onwards:
##  filter(date(ReadingDate) >="2016-08-25" & date(ReadingDate) <="2016-09-05" & site=="Punggol")
## 3090005152M, PG_B1 MainMeters consumption stops on 4 October 2016.

load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/PunggolConsumption.RData")
rm(PunggolConsumption_MAINSUB)
load("/srv/shiny-server/DataAnalyticsPortal/data/Net_Consumption.RData")

IndirectNetConsumption <- DirectNetConsumption <- TotalNetConsumption <- list()
IndirectMainMeters <- DirectMainMeters <- TotalMainMeters <-list()
IndirectSubMeters <- DirectSubMeters <- list()
IndirectNetConsumptionRate <- DirectNetConsumptionRate <- list()
TotalNetConsumptionRate <- list()

blocks <- c("PG_B1","PG_B2","PG_B3","PG_B4")

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

    NetConsumption_New <- cbind(Date=as.data.frame(rep(week.end,5)),
                            Block=as.data.frame(c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")),
                            IndirectMainMeters=as.data.frame(unlist(IndirectMainMeters)),
                            IndirectNetConsumption=as.data.frame(unlist(IndirectNetConsumption)),
                            DirectMainMeters=as.data.frame(unlist(DirectMainMeters)),
                            DirectNetConsumption=as.data.frame(unlist(DirectNetConsumption)),
                            TotalMainMeters=as.data.frame(unlist(TotalMainMeters)),
                            TotalNetConsumption=as.data.frame(unlist(TotalNetConsumption)),
                            TotalNetConsumptionRate=as.data.frame(unlist(TotalNetConsumptionRate)))
    
    NetConsumptionCSV_New <- cbind(Date=as.data.frame(rep(week.end,5)),
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

colnames(NetConsumption_New) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
                              "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
                              "TotalNetConsumptionRate")
colnames(NetConsumptionCSV_New) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
                                     "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
                                     "TotalNetConsumption","TotalNetConsumptionRate")
    
NetConsumption = rbind(NetConsumption,NetConsumption_New)
NetConsumptionCSV = rbind(NetConsumptionCSV,NetConsumptionCSV_New)

save(NetConsumption,NetConsumptionCSV, file="/srv/shiny-server/DataAnalyticsPortal/data/Net_Consumption.RData")

# colnames(NetConsumption) <- c("Date","Block","IndirectMainMeters","IndirectNetConsumption",
#                               "DirectMainMeters","DirectNetConsumption","TotalMainMeters","TotalNetConsumption",
#                               "TotalNetConsumptionRate")
# colnames(NetConsumptionCSV) <- c("Date","Block","IndirectMainMeters","IndirectSubMeters","IndirectNetConsumption",
#                                  "DirectMainMeters","DirectSubMeters","DirectNetConsumption","TotalMainMeters",
#                                  "TotalNetConsumption","TotalNetConsumptionRate")

all_blocks <- c(blocks,"PG_B5")
for (i in 1:length(all_blocks)){
  assign(paste("NetConsumption_",all_blocks[i],sep=""), NetConsumption %>% filter(Block==all_blocks[i]))
}

NetConsumption_PG_B1 <- xts(NetConsumption_PG_B1$TotalNetConsumption,NetConsumption_PG_B1$Date)
NetConsumption_PG_B2 <- xts(NetConsumption_PG_B2$TotalNetConsumption,NetConsumption_PG_B2$Date)
NetConsumption_PG_B3 <- xts(NetConsumption_PG_B3$TotalNetConsumption,NetConsumption_PG_B3$Date)
NetConsumption_PG_B4 <- xts(NetConsumption_PG_B4$TotalNetConsumption,NetConsumption_PG_B4$Date)
NetConsumption_PG_B5 <- xts(NetConsumption_PG_B5$TotalNetConsumption,NetConsumption_PG_B5$Date)

for (i in 1:length(all_blocks)){
  assign(paste("NetConsumptionRate_",all_blocks[i],sep=""), NetConsumption %>% filter(Block==all_blocks[i]))
}

NetConsumptionRate_PG_B1 <- xts(NetConsumptionRate_PG_B1$TotalNetConsumptionRate,NetConsumptionRate_PG_B1$Date)
NetConsumptionRate_PG_B2 <- xts(NetConsumptionRate_PG_B2$TotalNetConsumptionRate,NetConsumptionRate_PG_B2$Date)
NetConsumptionRate_PG_B3 <- xts(NetConsumptionRate_PG_B3$TotalNetConsumptionRate,NetConsumptionRate_PG_B3$Date)
NetConsumptionRate_PG_B4 <- xts(NetConsumptionRate_PG_B4$TotalNetConsumptionRate,NetConsumptionRate_PG_B4$Date)
NetConsumptionRate_PG_B5 <- xts(NetConsumptionRate_PG_B5$TotalNetConsumptionRate,NetConsumptionRate_PG_B5$Date)

NetConsumption_xts <- cbind(NetConsumption_PG_B1,NetConsumption_PG_B2,
                            NetConsumption_PG_B3,NetConsumption_PG_B4,NetConsumption_PG_B5)
colnames(NetConsumption_xts)[1:5] <- c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5") 

NetConsumptionRate_xts <- cbind(NetConsumptionRate_PG_B1,NetConsumptionRate_PG_B2,
                                NetConsumptionRate_PG_B3,NetConsumptionRate_PG_B4,NetConsumptionRate_PG_B5)
colnames(NetConsumptionRate_xts)[1:5] <- c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")

colnames(NetConsumption) <- c("Date","Block","Indirect\nMainMeters","IndirectNet\nConsumption",
                              "Direct\nMainMeters","DirectNet\nConsumption","Total\nMainMeters","TotalNet\nConsumption",
                              "TotalNet\nConsumptionRate")

NetConsumption <- NetConsumption %>% dplyr::arrange(desc(Date))
NetConsumptionCSV <- NetConsumptionCSV %>% dplyr::arrange(desc(Date))

save(NetConsumption, NetConsumption_xts, NetConsumptionRate_xts, file="/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption.RData")
write.csv(NetConsumptionCSV,"/srv/shiny-server/DataAnalyticsPortal/data/NetConsumption.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NetConsumption_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
