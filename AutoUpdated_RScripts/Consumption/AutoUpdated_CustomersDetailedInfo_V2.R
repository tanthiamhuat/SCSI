rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table,mailR,fst)

lastmonth <- month(today())-1
if (lastmonth==0) {
  lastmonth=12
} 

this_year <- year(today())

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))) %>%
          group_by(id_service_point) %>%
          dplyr::filter(move_in_date==max(move_in_date))
          
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 
meter <- as.data.frame(tbl(con,"meter"))
flow <- as.data.frame(tbl(con,"flow"))
alarm <- as.data.frame(tbl(con,"leak_alarm"))
consumption_challenges <- as.data.frame(tbl(con,"consumption_challenges"))
leak_open <- as.data.frame(tbl(con,"leak_alarm")) %>% filter(status=="Open" & site=="Punggol")

family_consumption_challenges <- full_join(family,consumption_challenges,by=c("id"="family_id")) %>%
                                 dplyr::select_("id","id_service_point","move_in_date","move_out_date","num_house_member","room_type","status",
                                                "high_flow","high_shower_usage","pub_cust_id","online_status","highest_day","highest_week",
                                                "highest_period")

daily_occupancy <- as.data.frame(tbl(con,"daily_occupancy"))
weekly_occupancy <- as.data.frame(tbl(con,"weekly_occupancy"))
monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))

#load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD"))) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),month=month(adjusted_date)) %>%
  dplyr::filter(Year==this_year & month==lastmonth) %>%             
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date,Year,month) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

customer_monthlyconsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  group_by(id_service_point,service_point_sn,block,floor,unit,room_type) %>%
  dplyr::summarise(MonthlyConsumption=sum(adjusted_consumption,na.rm = TRUE))

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
CustomerDetailedInfo <- full_join(CustomerDetailedInfo,family_consumption_challenges,by=c("id_service_point","room_type")) %>%
                        dplyr::rename(family_id=id)

CustomerDetailedInfo["id_service_point"] <- NULL

CustomerDetailedInfo <- CustomerDetailedInfo %>%
                        dplyr::mutate(LeakOpen=ifelse(service_point_sn %in% leak_open$service_point_sn,TRUE,FALSE))

## to include ChildCare and Vacant
family_VACANT_ChildCare <- tbl(con,"family") %>% 
                           as.data.frame() %>%
                           dplyr::filter(status=="VACANT" | room_type=="HDBCD") 

family_VACANT_ChildCare_servicepoint <- inner_join(family_VACANT_ChildCare,servicepoint,by=c("id_service_point"="id","room_type")) %>%
                                        dplyr::select_("service_point_sn","block","floor","unit","room_type","status","high_flow","high_shower_usage","pub_cust_id","online_status") %>%
                                        dplyr::filter(!(service_point_sn %in% c("3100250262","3100159919","3100250198")))

CustomerDetailedInfo=plyr::rbind.fill(CustomerDetailedInfo,family_VACANT_ChildCare_servicepoint)

save(CustomerDetailedInfo,file='/srv/shiny-server/DataAnalyticsPortal/data/CustomerDetailedInfo.RData')

setwd("/srv/shiny-server/DataAnalyticsPortal/data")

filename <- paste("CustomerDetailedInfo_",month.name[lastmonth],this_year,".csv",sep="")
write.csv(CustomerDetailedInfo,filename)

subject_contents = paste("Customers Details Information for ",month.name[lastmonth]," ",this_year, sep = "")

send.mail(from = "shd-snfr-autoreport@suez.com",
          to = c("thiamhuat.tan@suez.com","noemie.laurent@suez.com","huiling.ang@suez.com"),
          subject = subject_contents,
          body = 'Customers Details Information is attached.\n
                  This email was sent automatically.\n
                  Please do not reply directly to this e-mail.',
          html = TRUE,
          inline = TRUE,
          smtp = list(host.name = "smtp.office365.com",
                      port = 587,
                      user.name = "thiamhuat.tan@suez.com",
                      passwd = "Joy03052007######",
                      tls = TRUE),
          attach.files = filename,
          authenticate = TRUE,
          send = TRUE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerDetailedInfo_V2 successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)

