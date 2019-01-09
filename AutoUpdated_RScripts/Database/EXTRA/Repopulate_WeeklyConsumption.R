## Repopulate weekly_consumption table

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

weekly_consumption_DB <- as.data.frame(tbl(con,"weekly_consumption"))

week_start <- as.Date("2018-10-08")  # 2018-10-08 week_number=2018_40
week_end <- as.Date("2018-10-08")    # 2018-10-15 week_number=2018_41
weeks_range <- seq(week_start,week_end,by=7)  

weekly_consumption_Final <- list()

for (i in 1:length(weeks_range))
{
  set.seed(1000) # for reproducible values
  date_specified <- weeks_range[i]
  source("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Forecast/OLD/Forecast Model_Weekly_Indiv_Consumption_ML_SpecificWeek.R")

  weekly_consumption <- Forecast_Consumption[,c("service_point_sn","week_number","Consumption","Forecast.Consumption")] 

  weekly_occupancyrate <- as.data.frame(tbl(con,"weekly_occupancy")) %>% 
                          filter(week_number==week2predict) %>%
                          dplyr::mutate(occupiedDays=round(occupancy_rate*7/100)) 

  weekly_consumption_tmp <- left_join(weekly_consumption,weekly_occupancyrate,by=c("service_point_sn","week_number")) 
  weekly_consumption <- weekly_consumption_tmp %>% 
        dplyr::mutate(Forecast.Consumption_OccupancyRate=round(Forecast.Consumption*occupancy_rate/100)) %>%
        # Forecast.Consumption_OccupancyRate is the Forecast.Consumption take into account of the occupancy rate
        dplyr::select_("service_point_sn","week_number","Consumption","Forecast.Consumption","Forecast.Consumption_OccupancyRate")

  weekly_consumption <- weekly_consumption %>%
      dplyr::mutate(water_savings=round(Forecast.Consumption_OccupancyRate-Consumption),
                    water_savings=ifelse(water_savings<0,0,water_savings),
                    water_savings_ratio=water_savings/Consumption) 
  weekly_consumption$water_savings_ratio[which(weekly_consumption$Consumption==0)] <- 0
  weekly_consumption$water_savings_ratio <- pmin(weekly_consumption$water_savings_ratio,1)
  weekly_consumption$water_savings_ratio[which(weekly_consumption$water_savings_ratio>0.70)] <- weekly_consumption$water_savings_ratio[which(weekly_consumption$water_savings_ratio>0.70)]/2
  weekly_consumption$water_savings_corrected1 <- weekly_consumption$Consumption * weekly_consumption$water_savings_ratio
  weekly_consumption["Forecast.Consumption_OccupancyRate"] <- NULL
  
  stability.coef <- Indiv %>% group_by(service_point_sn) %>%
      dplyr::summarise(sd=sd(Consumption),avg=mean(Consumption)) %>%
      dplyr::mutate(Stability.Coef=sd/avg)
  stability.coef$Stability.Coef[which(stability.coef$avg == 0)] <- 0
  # stability.coef$Stability.Coef <- stability.coef$Stability.Coef/max(stability.coef$Stability.Coef)
  limit.stab <- quantile(stability.coef$Stability.Coef,3/4)
  stability.coef$Coeff <- ifelse(stability.coef$Stability.Coef>limit.stab,1/2,1)

  weekly_consumption$stability.coef <- stability.coef$Coeff[match(weekly_consumption$service_point_sn,stability.coef$service_point_sn)]
  weekly_consumption$stability.coef[which(weekly_consumption$Consumption==0)] <- 0
  weekly_consumption$water_savings_corrected2 <- weekly_consumption$water_savings_corrected1 * weekly_consumption$stability.coef

  weekly_consumption$water_savings_FINAL <- round(weekly_consumption$water_savings_corrected2/7)

  weekly_consumption <- weekly_consumption %>% dplyr::select_("service_point_sn","week_number","Consumption","Forecast.Consumption","water_savings_FINAL")
  colnames(weekly_consumption)[colnames(weekly_consumption)=="water_savings_FINAL"] <- "water_savings"

  # reference the room_type from family table.
  family <- as.data.frame(tbl(con,"family"))  # 560, include duplicates of service_point_sn with old and new customers
  family <- family %>% group_by(id_service_point) %>% filter(move_in_date==max(move_in_date)) 
  servicepoint <- as.data.frame(tbl(con,"service_point")) %>% filter(service_point_sn !="3101127564") # exclude ChildCare
  family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) %>% 
    dplyr::select_("service_point_sn","num_house_member")  
  family_servicepoint["id_service_point"] <- NULL

  weekly_consumption_family <- inner_join(weekly_consumption,family_servicepoint,by=c("service_point_sn")) 

  weekly_consumption_family$occupiedDays <- round(weekly_occupancyrate$occupiedDays[match(weekly_consumption_family$service_point_sn,
                                                                                        weekly_occupancyrate$service_point_sn)])

  weeklyconsumption <- weekly_consumption_family %>%
    dplyr::mutate(LPCD=Consumption/(num_house_member*occupiedDays)) %>%
    dplyr::mutate(water_savings_lpcd=ifelse(water_savings==0,0, # water_savings_lpcd=0 when water_savings=0
                                     ifelse(LPCD>300,0,
                                     water_savings/(num_house_member*occupiedDays)))) 

  weeklyconsumption$water_savings_lpcd <- round(weeklyconsumption$water_savings_lpcd)

  weekly_consumption <- subset(weeklyconsumption,select = c("service_point_sn","week_number","Consumption","Forecast.Consumption",
                                                            "water_savings","water_savings_lpcd"))

  weekly_consumption <- weekly_consumption[,c("service_point_sn","week_number","Consumption","Forecast.Consumption",
                                            "water_savings","water_savings_lpcd")]
  colnames(weekly_consumption)[3] <- "actual_consumption"
  colnames(weekly_consumption)[4] <- "forecasted_consumption"

  weekly_consumption <- as.data.frame(weekly_consumption)

  ## IF Forecasted_consumption (weekly) > ( 1.25 x average weekly consumption for last4weeks), then =>> divide the water_savings_lpcd by 2.
  pastweeks <- 5
  startdate <- date_specified-weeks(pastweeks)
  enddate <- date_specified-weeks(2)

  weekly_consumption_tmp <- inner_join(weekly_consumption_DB,Week.date,by=c("week_number"="week"))

  weekly_consumption_lastfewweeks <- weekly_consumption_tmp %>% dplyr::filter(end>=startdate & beg <=enddate) %>%
    dplyr::group_by(service_point_sn) %>%
    dplyr::summarise(Average=round(mean(actual_consumption)))

  threshold = 6  
  weekly_consumption_tmp <- inner_join(weekly_consumption,weekly_consumption_lastfewweeks,by="service_point_sn") %>%
                            dplyr::mutate(Ratio=forecasted_consumption/Average) %>%
                            dplyr::filter(Ratio > 1.25 & water_savings_lpcd > threshold) 
  weekly_consumption_Final[[i]] <- weekly_consumption %>% dplyr::mutate(water_savings_lpcd=ifelse(service_point_sn %in% weekly_consumption_tmp$service_point_sn,
                                                                                           round(water_savings_lpcd/2),round(water_savings_lpcd)))
}

weekly_consumption_Final <- as.data.frame(rbindlist(weekly_consumption_Final))

total_rows <- nrow(weekly_consumption_Final)

  weekly_consumption_Final$id <- as.integer(seq(max(weekly_consumption_DB$id)+1,
                                                     max(weekly_consumption_DB$id)+total_rows,1))
  
  weekly_consumption_Final <- weekly_consumption_Final[,c(ncol(weekly_consumption_Final),
                                                        c(1:ncol(weekly_consumption_Final)-1))]
  
  weekly_consumption_Final <- as.data.frame(weekly_consumption_Final)
  
  ## daily appended table
  dbWriteTable(mydb, "weekly_consumption", weekly_consumption_Final, append=TRUE, row.names=F, overwrite=FALSE) # append table
  dbDisconnect(mydb)

  
  weekly_consumption_DB_Count <- weekly_consumption_DB %>% dplyr::group_by(week_number) %>% dplyr::summarise(Count=n())
  