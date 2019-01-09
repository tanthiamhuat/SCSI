rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

library(RPostgreSQL)
library(plyr)
library(dplyr)
library(data.table)
library(sqldf)
library(lubridate)
#library(RODBC)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

consumption <- as.data.frame(tbl(con,"consumption"))

servicepoint <- as.data.frame(tbl(con,"service_point"))

consumption_servicepoint <- inner_join(consumption,servicepoint,by=c("id_service_point"="id"))
consumption_servicepoint$adjusted_consumption <- consumption_servicepoint$interpolated_consumption

consumption_names <-colnames(consumption_servicepoint)  

#Z <- consumption_servicepoint %>% filter(id_service_point==134)
#Z <- consumption_servicepoint %>% filter(id_service_point==269)
### Automatic adjusted consumptions
Adjusted.consumption <- function(Z){
    Z <- Z %>% arrange(date_consumption)
    Z$H <- hour(Z$date_consumption) 
    threshold <- pmax(quantile(Z$adjusted_consumption,0.99,na.rm=TRUE),1)*3
    abormalpeak <- which(Z$adjusted_consumption > threshold)
    while (length(abormalpeak) > 0)
    {
      Z$adjusted_consumption[abormalpeak] <- Z$adjusted_consumption[abormalpeak+1]
      abormalpeak <- which(Z$adjusted_consumption > threshold)
    }
    Z$diff.consumption <- abs(c(0,diff(Z$adjusted_consumption)))
    Z$diff.consumption[which(Z$diff.consumption==1)] <- 0
    Z$good.data <- 1
    temp <- rle(Z$diff.consumption)
    repeated.value <- which(temp$values == 0 & temp$lengths >=4)
      
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
          start.index <- ifelse(repeated.value[i]==1,1,sum(temp$lengths[1:(repeated.value[i]-1)]))
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
          start.index <- ifelse(repeated.value[i]==1,1,sum(temp$lengths[1:(repeated.value[i]-1)]))
          #if (sum(Z$adjusted_consumption[start.index:end.index]==0) == 0)
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
Correct.adjusted_consumption2 <- Correct.adjusted_consumption %>% filter(id_service_point %in% c("101","103","104","105","107"))

sql_update <- paste("UPDATE consumption SET adjusted_consumption = '",Correct.adjusted_consumption2$adjusted_consumption,"'
                     WHERE interpolated_consumption = '",Correct.adjusted_consumption2$interpolated_consumption,"' and
                     date_consumption = '",Correct.adjusted_consumption2$date_consumption,"' and
                     id_service_point = '",Correct.adjusted_consumption2$id_service_point,"' ",sep="")

sapply(sql_update, function(x){dbSendQuery(mydb, x)})

dbDisconnect(mydb)
print("adjusted_consumption column successfully written to Database")
