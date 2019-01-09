rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,mailR,RPushbullet)

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
                     dplyr::mutate(water_savings_lpcd=ifelse(water_savings==0,0, # water_savings_lpcd=0 when water_savings=0
                                                             water_savings/(num_house_member*occupiedDays))) 

weeklyconsumption$water_savings_lpcd <- round(weeklyconsumption$water_savings_lpcd)

weekly_consumption <- subset(weeklyconsumption,select = c("service_point_sn","week_number","Consumption","Forecast.Consumption",
                                                          "water_savings","water_savings_lpcd"))

number_customers <- length(weekly_consumption$service_point_sn)  # 523 customers

weekly_consumption$id <- as.integer(seq(max(weekly_consumption_DB$id)+1,
                                        max(weekly_consumption_DB$id)+number_customers,1))

weekly_consumption <- weekly_consumption[,c("id","service_point_sn","week_number","Consumption","Forecast.Consumption",
                                            "water_savings","water_savings_lpcd")]
colnames(weekly_consumption)[4] <- "acutal_consumption"
colnames(weekly_consumption)[5] <- "forecasted_consumption"

weekly_consumption <- as.data.frame(weekly_consumption)

dbWriteTable_output <- try(
  dbWriteTable(mydb, "weekly_consumption", weekly_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
  )
dbDisconnect(mydb)

if (class(dbWriteTable_output)=='try-error'){
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),#"benjamin.evain@suez.com"),
            subject = "weekly_consumption table Not Updated Properly",
            body = 'The weekly_consumption table is not updated properly.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suezenvironnement.com",
                        passwd = "Joy@03052007",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)
  
  pbPost("note", title="weekly_consumption table Not Updated Properly",
          apikey = 'o.3hyhI3lAxvjlB8p1I2Pu2sowkE7YNNqF') 
  
}

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Weekly_Consumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

