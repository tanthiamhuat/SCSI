rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

source("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Forecast/Forecast Model_Weekly_Indiv_Consumption_V2.R")

con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
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
                                water_savings=ifelse(water_savings<0,0,water_savings)) 
weekly_consumption["Forecast.Consumption_OccupancyRate"] <- NULL

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
                     dplyr::mutate(water_savings_lpcd=water_savings/(num_house_member*occupiedDays)) 

# water_savings_lpcd=0 when water_savings=0
# weeklysavings[weeklysavings$water_savings==0,] %<>% mutate(water_savings_lpcd = 0)

weeklyconsumption$water_savings_lpcd <- round(weeklyconsumption$water_savings_lpcd)

weekly_consumption <- subset(weeklyconsumption,select = c("service_point_sn","week_number","Consumption","Forecast.Consumption",
                                                          "water_savings","water_savings_lpcd"))

number_customers <- length(weekly_consumption$service_point_sn)  # 523 customers

weekly_consumption$id <- as.integer(seq(nrow(weekly_consumption_DB)+1,
                                       nrow(weekly_consumption_DB)+number_customers,1))

weekly_consumption <- weekly_consumption[,c("id","service_point_sn","week_number","Consumption","Forecast.Consumption",
                                            "water_savings","water_savings_lpcd")]
colnames(weekly_consumption)[4] <- "acutal_consumption"
colnames(weekly_consumption)[5] <- "forecasted_consumption"
weekly_consumption <- data.frame(weekly_consumption)  ## must be data.frame to be able to write to database

mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
dbWriteTable(mydb, "weekly_consumption", weekly_consumption, append=TRUE, row.names=F, overwrite=FALSE) # append table
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Weekly_Consumption successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

