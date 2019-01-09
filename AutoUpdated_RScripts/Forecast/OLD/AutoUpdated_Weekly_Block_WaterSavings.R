rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

source("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Forecast/Forecast Model_Weekly_Indiv_Consumption_V2.R")

weekly_consumption_DB <- as.data.frame(tbl(con,"weekly_consumption"))

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
                                water_savings_percent=water_savings/Consumption) 
weekly_consumption$water_savings_percent[which(weekly_consumption$Consumption==0)] <- 0
weekly_consumption$water_savings_percent <- pmin(weekly_consumption$water_savings_percent,1)
weekly_consumption$water_savings_percent[which(weekly_consumption$water_savings_percent>0.70)] <- weekly_consumption$water_savings_percent[which(weekly_consumption$water_savings_percent>0.70)]/2
weekly_consumption$water_savings_corrected1 <- weekly_consumption$Consumption * weekly_consumption$water_savings_percent
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

weekly_consumption$water_savings_FINAL <- weekly_consumption$water_savings_corrected2/7

