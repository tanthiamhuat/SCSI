## consumption table of status field comes with M: missing data, C: conmpensated data, R: real data
## ususally the compensated data is of very huge value, and before that compensated value, there is a series of missing data of zero.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

library(RPostgreSQL)
library(plyr)
library(dplyr)
library(data.table)
library(sqldf)
library(lubridate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))

consumption_last6months_Status <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_Status.fst",as.data.table=TRUE)
consumption_last6months_Status_ServicePoint <- inner_join(consumption_last6months_Status,servicepoint,by=c("id_service_point"="id")) %>%
                                               dplyr::select_("id_service_point","service_point_sn","interpolated_consumption","adjusted_consumption","date_consumption","status",
                                                              "block","floor","unit","room_type","site","meter_type")

consumption_last6months_Status_ServicePoint_SUB <- consumption_last6months_Status_ServicePoint %>% filter(meter_type=="SUB")


#Z <- consumption_servicepoint %>% filter(id_service_point==134)
#Z <- consumption_servicepoint %>% filter(id_service_point==269)
#Z <- consumption_servicepoint %>% filter(id_service_point==601)
Z <- consumption_last6months_Status_ServicePoint_SUB %>% filter(id_service_point==4)
### Automatic adjusted consumptions
Adjusted.consumption <- function(Z){
    Z <- Z %>% dplyr::filter(!is.na(adjusted_consumption)) %>% arrange(date_consumption)
    #id_tmp <<- Z$id_service_point[1]
    Z$H <- hour(Z$date_consumption) 
    threshold <- pmax(quantile(Z$adjusted_consumption,0.99,na.rm=TRUE),1)*3
    abormalpeak <- which(Z$adjusted_consumption > threshold)
    while (length(abormalpeak) > 0)
    {
      Z$adjusted_consumption[abormalpeak] <- Z$adjusted_consumption[abormalpeak+1]
      abormalpeak <- which(Z$adjusted_consumption > threshold)
    }
      Z$good.data <- 1
      temp <- rle(Z$status)
      repeated.value <- which(temp$values == "Interpolated value on missing index" & temp$lengths >=2)
      
    if (length(repeated.value)==0){
      isna <- which(is.na(Z$adjusted_consumption))
      if(length(isna)>0){
        REF <- Z %>% dplyr::filter(good.data==1) %>% filter(!is.na(adjusted_consumption))
        REF.hour <- REF %>% group_by(H) %>% dplyr::summarise(avg=mean(adjusted_consumption))
        REF.hour$ref.alloc <- REF.hour$avg/sum(REF.hour$avg)
        if(nrow(REF.hour)<24) {
          REF.hour <- data.frame(H=0:23,ref.alloc=1/24) # to be improved with traditional consumption pattern 
        }        
        Z$day <- ymd(substr(as.character(Z$date_consumption),1,10))
        day.na <- unique(Z$day[isna])
        for(d in day.na){
          temp <- Z %>% filter(day==d)
          navalue <- which(is.na(temp$adjusted_consumption))
          hourna <- temp$H[navalue]
          partial.consumption <- sum(temp$adjusted_consumption[-navalue])
          partial.allocation <- sum(REF.hour$ref.alloc[-match(hourna,REF.hour$H)])
          total.consumption <- partial.consumption/partial.allocation
          
          Z$adjusted_consumption[match(temp$date_consumption[navalue],Z$date_consumption)] <- total.consumption*REF.hour$ref.alloc[match(hourna,REF.hour$H)]
        }
      }
      return(Z[,consumption_names])
    }else{
        for(i in 1:length(repeated.value)){
          end.index <- sum(temp$lengths[1:repeated.value[i]])
          start.index <- ifelse(repeated.value[i]==1,1,sum(temp$lengths[1:(repeated.value[i]-1)])) + 2 ## plus 2 added by Thiam Huat
          Z$good.data[start.index:end.index] <- 0
        }
        
        REF <- Z %>% dplyr::filter(good.data==1) %>% filter(!is.na(adjusted_consumption))
        REF.hour <- REF %>% group_by(H) %>% dplyr::summarise(avg=mean(adjusted_consumption))
        REF.hour$ref.alloc <- REF.hour$avg/sum(REF.hour$avg)
        if(nrow(REF.hour)<24 | sum(is.nan(REF.hour$ref.alloc))>0) {
          REF.hour <- data.frame(H=0:23,ref.alloc=1/24) # to be improved with traditional consumption pattern 
        }
        Z$ref.alloc <- REF.hour$ref.alloc[match(Z$H,REF.hour$H)]
        for(i in 1:length(repeated.value)){
          end.index <- sum(temp$lengths[1:repeated.value[i]])
          start.index <- ifelse(repeated.value[i]==1,1,sum(temp$lengths[1:(repeated.value[i]-1)])) + 2 ## plus 2 added by Thiam Huat
          #if (sum(Z$adjusted_consumption[start.index:end.index]==0) == 0)
          #if (any(Z$adjusted_consumption[start.index:end.index] !=0))  -> some error !!
          if (any(Z$adjusted_consumption[start.index:end.index] !=0))
          {
            tot.consumption.period <- sum(Z$adjusted_consumption[start.index:end.index])
            re.allocation <- Z$ref.alloc[start.index:end.index]/sum(Z$ref.alloc[start.index:end.index])
            Z$adjusted_consumption[start.index:end.index] <- round(tot.consumption.period * re.allocation)
          }
        }
        isna <- which(is.na(Z$adjusted_consumption))
        if(length(isna)>0){
          Z$day <- ymd(substr(as.character(Z$date_consumption),1,10))
          day.na <- unique(Z$day[isna])
          for(d in day.na){
            temp <- Z %>% filter(day==d)
            navalue <- which(is.na(temp$adjusted_consumption))
            hourna <- temp$H[navalue]
            partial.consumption <- sum(temp$adjusted_consumption[-navalue])
            partial.allocation <- sum(REF.hour$ref.alloc[-match(hourna,REF.hour$H)])
            total.consumption <- partial.consumption/partial.allocation
            
            Z$adjusted_consumption[match(temp$date_consumption[navalue],Z$date_consumption)] <- total.consumption*REF.hour$ref.alloc[match(hourna,REF.hour$H)]
          }
        }
        return(Z[,consumption_names])
    }
}

Correct.adjusted_consumption <- ddply(.data = consumption_servicepoint,.variables = .(id_service_point),.fun = Adjusted.consumption,.progress = 'text')
Correct.adjusted_consumption <- Correct.adjusted_consumption[,1:11]
colnames(Correct.adjusted_consumption)[7] <- "updated_date"
Correct.adjusted_consumption <- Correct.adjusted_consumption %>% arrange(id)

## update table using adjusted_consumption != interpolated_consumption
Correct.adjusted_consumption1 <- Correct.adjusted_consumption %>% filter(adjusted_consumption != interpolated_consumption)
#Correct.adjusted_consumption2 <- Correct.adjusted_consumption %>% filter(id_service_point %in% c("101","103","104","105","107"))
Correct.adjusted_consumption3 <- Correct.adjusted_consumption %>% filter(id_service_point == "293")

sql_update <- paste("UPDATE consumption SET adjusted_consumption = '",Correct.adjusted_consumption1$adjusted_consumption,"'
                     WHERE interpolated_consumption = '",Correct.adjusted_consumption1$interpolated_consumption,"' and
                     date_consumption = '",Correct.adjusted_consumption1$date_consumption,"' and
                     id_service_point = '",Correct.adjusted_consumption1$id_service_point,"' ",sep="")

sapply(sql_update, function(x){dbSendQuery(mydb, x)})

dbDisconnect(mydb)
print("adjusted_consumption column successfully written to Database")
