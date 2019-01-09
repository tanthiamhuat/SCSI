rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,lubridate,data.table,mailR)

lastmonth <- month(today())-1
if (lastmonth==0) {
  lastmonth=12
} 

this_year <- year(today())

con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

consumption <- as.data.frame(tbl(con,"consumption"))
servicepoint <- as.data.frame(tbl(con,"service_point"))
meter <- as.data.frame(tbl(con,"meter"))
flow <- as.data.frame(tbl(con,"flow"))
alarm <- as.data.frame(tbl(con,"leak_alarm"))
consumption_challenges <- as.data.frame(tbl(con,"consumption_challenges"))
family <- as.data.frame(tbl(con,"family"))
leak_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site=="Punggol")

family_consumption_challenges <- inner_join(family,consumption_challenges,by=c("id"="family_id")) %>%
                                 dplyr::select_("id","id_service_point","move_in_date","move_out_date","num_house_member","room_type","status",
                                                "high_flow","high_shower_usage","pub_cust_id","online_status","highest_day","highest_week",
                                                "highest_period")

daily_occupancy <- as.data.frame(tbl(con,"daily_occupancy"))
weekly_occupancy <- as.data.frame(tbl(con,"weekly_occupancy"))
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))

consumption_servicepoint <- inner_join(consumption,servicepoint,by=c("id_service_point"="id"))
consumption_servicepoint_meter <- inner_join(consumption_servicepoint,meter,by=c("service_point_sn"="id_real_estate"))

customer_monthlyconsumption <- consumption_servicepoint_meter %>% 
                               filter(room_type!="HDBCD" & room_type!="NIL") %>%
                               mutate(Month=month(date_consumption)) %>%
                               filter(Month==lastmonth) %>%
                               group_by(id_service_point,service_point_sn,block,floor,unit,room_type) %>%
                               dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption,na.rm=TRUE))
         
customer_flow <- flow %>% 
                 mutate(Month=month(flow_date),Year=year(flow_date)) %>%
                 filter(Month==lastmonth & Year==this_year & id_service_point %in% customer_monthlyconsumption$id_service_point) %>%
                 group_by(id_service_point) %>%
                 dplyr::summarise(AvgMinFlow5=round(mean(min_5_flow)),AvgMaxFlow5=round(mean(max_5_flow)),AvgMaxFlow15=round(mean(max_15_flow)))

customer_dailyoccupancy <- daily_occupancy %>%
                           mutate(Month=month(date_consumption),Year=year(date_consumption)) %>%
                           filter(Month==lastmonth & Year==this_year & service_point_sn %in% customer_monthlyconsumption$service_point_sn) %>%
                           group_by(service_point_sn) %>%
                           dplyr::summarise(AverageDailyOccupancy=round(mean(occupancy_rate)))

customer_weeklyoccupancy <- weekly_occupancy %>%
                            mutate(Month=month(lastupdated),Year=year(lastupdated)) %>%
                            filter(Month==lastmonth & Year==this_year & service_point_sn %in% customer_monthlyconsumption$service_point_sn) %>%
                            group_by(service_point_sn) %>%
                            dplyr::summarise(AverageWeeklyOccupancy=round(mean(occupancy_rate)))

customer_monthlyoccupancy <- monthly_occupancy %>%
                             mutate(Month=month(lastupdated),Year=year(lastupdated)) %>%
                             dplyr::filter(Month==lastmonth & Year==this_year & service_point_sn %in% customer_monthlyconsumption$service_point_sn) %>%
                             group_by(service_point_sn) %>%
                             dplyr::summarise(AverageMonthlyOccupancy=round(mean(occupancy_rate)))
                            
CustomerDetailedInfo <- inner_join(customer_monthlyconsumption,customer_dailyoccupancy,by="service_point_sn")
CustomerDetailedInfo <- inner_join(CustomerDetailedInfo,customer_weeklyoccupancy,by="service_point_sn")
CustomerDetailedInfo <- inner_join(CustomerDetailedInfo,customer_monthlyoccupancy,by="service_point_sn")
CustomerDetailedInfo <- inner_join(CustomerDetailedInfo,customer_flow,by="id_service_point")
CustomerDetailedInfo <- inner_join(CustomerDetailedInfo,family_consumption_challenges,by=c("id_service_point","room_type")) %>%
                        dplyr::rename(family_id=id)

CustomerDetailedInfo["id_service_point"] <- NULL

CustomerDetailedInfo <- CustomerDetailedInfo %>%
                        dplyr::mutate(LeakOpen=ifelse(service_point_sn %in% leak_open$service_point_sn,TRUE,FALSE))

## to include customers with zero consumption.

setwd("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/Consumption")

filename <- paste("CustomerDetailedInfo_",month.name[lastmonth],this_year,".csv",sep="")
write.csv(CustomerDetailedInfo,filename)

subject_contents = paste("Customers Details Information for ",month.name[lastmonth]," ",this_year, sep = "")

send.mail(from = "shd-snfr-autoreport@suez.com",
          to = c("thiamhuat.tan@suez.com","benjamin.evain@suez.com","huiling.ang@suez.com"),
          subject = subject_contents,
          body = 'Customers Details Information is attached.\n
                  This email was sent automatically.\n
                  Please do not reply directly to this e-mail.',
          html = TRUE,
          inline = TRUE,
          smtp = list(host.name = "smtp.office365.com",
                      port = 587,
                      user.name = "thiamhuat.tan@suezenvironnement.com",
                      passwd = "Joy@03052007",
                      tls = TRUE),
          attach.files = filename,
          authenticate = TRUE,
          send = TRUE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerDetailedInfo successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

