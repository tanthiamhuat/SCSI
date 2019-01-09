## Repopulate daily_consumption table, for Punggol and Yuhua

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,fst)

# Establish connection
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

daily_consumption_DB <- as.data.frame(tbl(con,"daily_consumption"))
start_date <- as.Date("2018-05-01")
end_date <- as.Date("2018-05-10")
#start_date <- today()-5
#end_date <- today()-1

daily_consumption_DB_extracted <- daily_consumption_DB %>% 
                                  dplyr::filter(date_consumption>=start_date & date_consumption<=end_date)

daily_consumption_DB_extracted <- daily_consumption_DB_extracted[-1]
daily_consumption_DB_extracted$nett_consumption <- as.numeric(daily_consumption_DB_extracted$nett_consumption)
daily_consumption_DB_extracted$overconsumption <- as.numeric(daily_consumption_DB_extracted$overconsumption)
daily_consumption_DB_extracted$date_consumption <- as.Date(daily_consumption_DB_extracted$date_consumption) + days(1)

days_range <- seq(start_date,end_date,by=1)  ## daily_consumption: date_consumption from 2017-10-25 to 2017-10-31

daily_consumption_list <- list()

## from /data/DT directory for Punggol
Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/Punggol_last90days.fst",as.data.table=T)
Punggol_SUB <- Punggol_All[!room_type %in% c("NIL","HDBCD") & adjusted_consumption != "NA"] # contain data on sub meter, excluding childcare

## from /data/DT directory for Yuhua
Yuhua_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/Yuhua_last90days.fst",as.data.table=T)
Yuhua_SUB <- Yuhua_All[meter_type == "SUB" & !room_type %in% c("NIL","OTHER") & interpolated_consumption != "NA"] # contain data on sub meter, excluding shophouse

PunggolYuhua_SUB <- rbind(Punggol_SUB,Yuhua_SUB)

for (i in 1:length(days_range))
{
  family <- as.data.frame(tbl(con,"family")) %>% 
            dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
  servicepoint <- as.data.frame(tbl(con,"service_point"))
  family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

  PunggolYuhuaConsumption <- inner_join(PunggolYuhua_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) 

  date_past3months <- days_range[i]+days(1)-days(90)

  DailyConsumption <- PunggolYuhuaConsumption %>%
    dplyr::mutate(Date=date(date_consumption),wd=weekdays(Date)) %>%
    group_by(service_point_sn,wd,Date) %>%
    dplyr::summarise(DailyConsumption=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
    dplyr::filter(service_point_sn !="3101127564" & n==24) # remove Child-Care, full count of 24 per day

# average of the same day (e.g if today is Wednesday, then all the same 12* Wednesday) of last 3 months (individual) 
# extract daily consumption, combine with weekdays.
  DailyConsumption_past3months <- DailyConsumption %>%
    group_by(service_point_sn,wd) %>%
    dplyr::filter(wd==weekdays(days_range[i]+days(1)-1) & Date >= date_past3months) %>%
    dplyr::summarise(average_consumption_past3months=round(mean(DailyConsumption)))

last7days <- c(days_range[i]+days(1)-c(1:7))

DailyPunggolYuhuaConsumption_last7days <- PunggolYuhuaConsumption %>%
  dplyr::mutate(Date=date(date_consumption)) %>%
  dplyr::filter(Date >= move_in_date) %>%  # take into consideration of move-in-date
  dplyr::mutate(wd=weekdays(Date)) %>%
  group_by(service_point_sn,Date,wd) %>%
  dplyr::filter(Date %in% last7days) %>%
  dplyr::summarise(dailyconsumption_last7days=sum(interpolated_consumption,na.rm=TRUE),n = n()) %>%
  dplyr::filter(service_point_sn !="3101127564" & n==24) # exclude ChildCare, full count of 24 per day

threshold=50
daily_consumption_list[[i]] <- inner_join(DailyPunggolYuhuaConsumption_last7days,DailyConsumption_past3months,
                                     by=c("service_point_sn","wd")) %>%
  group_by(service_point_sn,wd,Date) %>%
  dplyr::mutate(overconsumption=dailyconsumption_last7days-(1+(threshold/100))*average_consumption_past3months) %>%
  dplyr::mutate(overconsumption=round(ifelse(overconsumption<0,0,overconsumption))) %>%
  dplyr::mutate(nett_consumption=round(dailyconsumption_last7days-overconsumption)) %>%
  dplyr::filter(Date==days_range[i]+days(1)-1) %>%  
  dplyr::select_("service_point_sn","nett_consumption","overconsumption","Date") 
}

daily_consumption <- as.data.frame(rbindlist(daily_consumption_list))

  leak_alarm_YH <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Leak_Yuhua.csv")
  leak_alarm_YH_open <- leak_alarm_YH %>% filter(status=="Open")
  
  leak_alarm_PG <- as.data.frame(tbl(con,"leak_alarm"))
  leak_alarm_PG_open <- leak_alarm_PG %>% dplyr::filter(status=="Open" & site=="Punggol")
  
  leak_alarm_open <- c(leak_alarm_PG_open$service_point_sn,leak_alarm_YH_open$service_point_sn)

  daily_consumption <- daily_consumption %>% 
                       dplyr::mutate(is_leak=ifelse(service_point_sn %in% leak_alarm_open,TRUE,FALSE)) 
  
  daily_consumption["wd"] <- NULL
  colnames(daily_consumption)[4] <- "date_consumption"
 
  daily_consumption <- daily_consumption[!duplicated(daily_consumption),] # remove duplicates 
 
  # difference btw 2 dataframe/data.table
  setdiffDF <- function(A, B){
    f <- function(A, B)
      A[!duplicated(rbind(B, A))[nrow(B) + 1:nrow(A)], ]
    df1 <- f(A, B)
    df2 <- f(B, A)
    rbind(df1, df2)
  }
  
  if (nrow(daily_consumption)>nrow(daily_consumption_DB_extracted)){
      daily_consumption_diff<- setdiffDF(daily_consumption,daily_consumption_DB_extracted)
 
      # remove rows with same date
      daily_consumption_diff_unique <- daily_consumption_diff %>% dplyr::group_by(service_point_sn,date_consumption) %>%
                                       dplyr::mutate(Count=n()) %>% 
                                       dplyr::filter(Count==1)
      daily_consumption_diff_unique["Count"] <- NULL
  
      total_rows <- nrow(daily_consumption_diff_unique)
      if (total_rows>0){
      daily_consumption_diff_unique$id <- as.integer(seq(max(daily_consumption_DB$id)+1,
                                                 max(daily_consumption_DB$id)+total_rows,1))

      daily_consumption_diff_unique <- daily_consumption_diff_unique[,c(ncol(daily_consumption_diff_unique),
                                                                  c(1:ncol(daily_consumption_diff_unique)-1))]

      daily_consumption_diff_unique <- as.data.frame(daily_consumption_diff_unique)

      ## daily appended table
      dbWriteTable(proddb, "daily_consumption", daily_consumption_diff_unique, append=TRUE, row.names=F, overwrite=FALSE) # append table
      dbDisconnect(proddb)
      }
  }

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_RepopulateDailyConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)
  
cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)