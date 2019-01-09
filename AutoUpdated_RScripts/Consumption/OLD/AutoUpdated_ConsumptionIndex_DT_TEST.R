gc()
rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,dplyr,data.table,lubridate,stringr,ISOweek,fst)

## to be run only one time to save the old data
# x=Sys.info()
# if (x[which(names(x)=='user')]=="thiamhuat")
# {
#   con1 <- dbConnect(PostgreSQL(),host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
# } else if (x[which(names(x)=='user')]=="dapuser") {
#   con1 <- dbConnect(PostgreSQL(),host = "10.206.13.72", user = "dapuser", password = "dapuser1234##", dbname="amrstaging")
# }
# consumption_2016 <- dbSendQuery(con1, "SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
#                                        FROM consumption
#                                        WHERE date(date_consumption) >= '2016-01-01' AND date(date_consumption) <= '2016-12-31'")
# consumption_2016 <- as.data.table(dbFetch(consumption_2016))
# consumption_2017 <- dbSendQuery(con1, "SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
#                                        FROM consumption
#                                        WHERE date(date_consumption) >= '2017-01-01' AND date(date_consumption) <= '2017-12-31'")
# consumption_2017 <- as.data.table(dbFetch(consumption_2017))
# write.fst(consumption_2016,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2016.fst",100)
# write.fst(consumption_2017,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_2017.fst",100)
# dbDisconnect(con1)
## to be run only one time to save the old data

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

last30days <- today()-30
last90days <- today()-90
last6months <- today()-180
last12months <- today()-360

thisyear <- year(today())
thisyear_start <- paste(thisyear,"-01-01",sep="")

x=Sys.info()
if (x[which(names(x)=='user')]=="thiamhuat")
{
  con1 <- dbConnect(PostgreSQL(),host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
} else if (x[which(names(x)=='user')]=="dapuser") {
  con1 <- dbConnect(PostgreSQL(),host = "10.206.13.72", user = "dapuser", password = "dapuser1234##", dbname="amrstaging")
}
consumption_last30days <- dbSendQuery(con1, paste("SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
                                                 FROM consumption
                                                 WHERE date(date_consumption) >= '",last30days,"'",sep=""))
consumption_last30days <- as.data.table(dbFetch(consumption_last30days))
consumption_last90days <- dbSendQuery(con1, paste("SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption,index_value
                                                 FROM consumption
                                                 WHERE date(date_consumption) >= '",last90days,"'",sep=""))  ## Need index_value for Data Download

consumption_last90days <- as.data.table(dbFetch(consumption_last90days))
consumption_last6months <- dbSendQuery(con1, paste("SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
                                                 FROM consumption
                                                 WHERE date(date_consumption) >= '",last6months,"'",sep=""))
consumption_last6months <- as.data.table(dbFetch(consumption_last6months))
consumption_last12months <- dbSendQuery(con1, paste("SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
                                                 FROM consumption
                                                 WHERE date(date_consumption) >= '",last12months,"'",sep=""))
consumption_last12months <- as.data.table(dbFetch(consumption_last12months))
consumption_thisyear <- dbSendQuery(con1, paste("SELECT id_service_point,interpolated_consumption,adjusted_consumption,date_consumption
                                                 FROM consumption
                                                 WHERE date(date_consumption) >= '",thisyear_start,"'",sep=""))
consumption_thisyear <- as.data.table(dbFetch(consumption_thisyear))

servicepoint <- as.data.table(tbl(con,"service_point"))

# set the ON clause as keys of the tables:
setkey(consumption_last30days,id_service_point)
setkey(consumption_last90days,id_service_point)
setkey(consumption_last6months,id_service_point)
setkey(consumption_last12months,id_service_point)
setkey(consumption_thisyear,id_service_point)
setkey(servicepoint,id)

consumption_last30days_servicepoint <- consumption_last30days[servicepoint, nomatch=0]
consumption_last30days_servicepoint <- consumption_last30days_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                               interpolated_consumption,adjusted_consumption,date_consumption)]
consumption_last90days_servicepoint <- consumption_last90days[servicepoint, nomatch=0]
consumption_last90days_servicepoint <- consumption_last90days_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                               interpolated_consumption,adjusted_consumption,date_consumption,index_value)]
## Above need index_value for Data Download
consumption_last6months_servicepoint <- consumption_last6months[servicepoint, nomatch=0]
consumption_last6months_servicepoint <- consumption_last6months_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                               interpolated_consumption,adjusted_consumption,date_consumption)]

consumption_last12months_servicepoint <- consumption_last12months[servicepoint, nomatch=0]
consumption_last12months_servicepoint <- consumption_last12months_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                                 interpolated_consumption,adjusted_consumption,date_consumption)]

consumption_thisyear_servicepoint <- consumption_thisyear[servicepoint, nomatch=0]
consumption_thisyear_servicepoint <- consumption_thisyear_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                                   interpolated_consumption,adjusted_consumption,date_consumption)]

write.fst(consumption_last30days_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last30days_servicepoint.fst",100)
write.fst(consumption_last90days_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last90days_servicepoint.fst",100)
write.fst(consumption_last6months_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_servicepoint.fst",100)
write.fst(consumption_last12months_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last12months_servicepoint.fst",100)
write.fst(consumption_thisyear_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_thisyear_servicepoint.fst",100)

Punggol_last6months <- consumption_last6months_servicepoint[site == "Punggol"]
Punggol_last6months <- Punggol_last6months[,Date.Time:=round_date(ymd_hms(date_consumption),"hour")]
Punggol_last6months <- Punggol_last6months[,c("Date","H","D","M","Y","wd","week","Consumption"):= list
                                           (date(Date.Time),
                                            hour(Date.Time),
                                            day(Date.Time),
                                            month(Date.Time),
                                            year(Date.Time),
                                            weekdays(Date.Time),
                                            gsub("-W","_",str_sub(date2ISOweek(date(Date.Time)),end = -3)),
                                            adjusted_consumption)]

Punggol_last90days <- consumption_last90days_servicepoint[site == "Punggol"]
Punggol_last90days <- Punggol_last90days[,Date.Time:=round_date(ymd_hms(date_consumption),"hour")]
Punggol_last90days <- Punggol_last90days[,c("Date","H","D","M","Y","wd","week","Consumption"):= list
                                           (date(Date.Time),
                                           hour(Date.Time),
                                           day(Date.Time),
                                           month(Date.Time),
                                           year(Date.Time),
                                           weekdays(Date.Time),
                                           gsub("-W","_",str_sub(date2ISOweek(date(Date.Time)),end = -3)),
                                           adjusted_consumption)]

Yuhua_last6months <- consumption_last6months_servicepoint[site == "Yuhua"]
Yuhua_last6months <- Yuhua_last6months[,Date.Time:=round_date(ymd_hms(date_consumption),"hour")]
Yuhua_last6months <- Yuhua_last6months[,c("Date","H","D","M","Y","wd","week","Consumption"):= list
                                           (date(date_consumption),
                                           hour(date_consumption),
                                           day(date_consumption),
                                           month(date_consumption),
                                           year(date_consumption),
                                           weekdays(date_consumption),
                                           gsub("-W","_",str_sub(date2ISOweek(date(date_consumption)),end = -3)),
                                           adjusted_consumption)]

Yuhua_last90days <- consumption_last90days_servicepoint[site == "Yuhua"]
Yuhua_last90days <- Yuhua_last90days[,Date.Time:=round_date(ymd_hms(date_consumption),"hour")]
Yuhua_last90days <- Yuhua_last90days[,c("Date","H","D","M","Y","wd","week","Consumption"):= list
                                       (date(date_consumption),
                                       hour(date_consumption),
                                       day(date_consumption),
                                       month(date_consumption),
                                       year(date_consumption),
                                       weekdays(date_consumption),
                                       gsub("-W","_",str_sub(date2ISOweek(date(date_consumption)),end = -3)),
                                       adjusted_consumption)]

write.fst(Punggol_last6months,"/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_last6months.fst",100)
write.fst(Yuhua_last6months,"/srv/shiny-server/DataAnalyticsPortal/data/DT/Yuhua_last6months.fst",100)
write.fst(Punggol_last90days,"/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_last90days.fst",100)
write.fst(Yuhua_last90days,"/srv/shiny-server/DataAnalyticsPortal/data/DT/Yuhua_last90days.fst",100)

## Index for Data Download
index_last90days <- dbSendQuery(con1, paste("SELECT id_service_point,current_index_date,index
                                             FROM index
                                             WHERE date(current_index_date) >= '",last90days,"'",sep=""))
index_last90days <- as.data.table(dbFetch(index_last90days))
dbDisconnect(con1)

# set the ON clause as keys of the tables:
setkey(index_last90days,id_service_point)
setkey(servicepoint,id)

index_last90days_servicepoint <- index_last90days[servicepoint, nomatch=0]
index_last90days_servicepoint <- index_last90days_servicepoint[, .(service_point_sn,block,floor,unit,room_type,site,meter_type,
                                                                   current_index_date,index)]

write.fst(index_last90days_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/index_last90days_servicepoint.fst",100)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Consumption_DT successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)

## write.fst(consumption_last90days_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last90days_servicepoint.fst",100)
## write.fst(index_last90days_servicepoint,"/srv/shiny-server/DataAnalyticsPortal/data/DT/index_last90days_servicepoint.fst",100)
## above 2 are used by AutoUpdated_ConsumptionIndexDownload_DT.R
