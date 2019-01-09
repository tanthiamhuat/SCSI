rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,dplyr,xlsx,data.table,lubridate)

last30day <- today()-30

DB_Connections_output <- try(
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
)

if (class(DB_Connections_output)=='try-error'){
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')
  source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
}

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")  # from DB
consumption_DB <- DB_Consumption %>% 
  dplyr::mutate(date=date(date_consumption)) %>%
  dplyr::filter(date>last30day) 

servicepoint <- tbl(con,"service_point") %>% 
  dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B") %>%
  as.data.frame()

meter <- as.data.frame(tbl(con,"meter")) %>% dplyr::filter(status=="ACTIVE")
  
family_vacant <- tbl(con,"family") %>% 
                 dplyr::filter(status=="VACANT") %>%
                 dplyr::select_("id_service_point","pub_cust_id","move_in_date","move_out_date") %>% 
                 dplyr::filter(is.na(move_in_date) & is.na(move_out_date)) %>%
                 as.data.frame()
family_vacant_servicepoint <- inner_join(family_vacant,servicepoint,by=c("id_service_point"="id")) %>% 
                              dplyr::select_("service_point_sn","block","pub_cust_id")

family_vacant_servicepoint_meter <- inner_join(family_vacant_servicepoint,meter,by=c("service_point_sn"="id_real_estate")) %>%
                                    dplyr::select_("service_point_sn","meter_sn","block","pub_cust_id")

monthly_occupancy <- tbl(con,"monthly_occupancy") %>% as.data.frame()
family_vacant_servicepoint_meter_monthly_occupancy <- inner_join(monthly_occupancy,family_vacant_servicepoint_meter,by="service_point_sn")

consumption_DB <- inner_join(consumption_DB,servicepoint,by=c("id_service_point"="id")) %>%
  dplyr::select_("service_point_sn","interpolated_consumption","index_value","date_consumption") 

## Check for Zero Total Interpolated Consumption (Start to End),
ZeroTotalConsumption <- consumption_DB %>%
                        group_by(service_point_sn) %>%
                        dplyr::summarize(total_consumption = sum(interpolated_consumption,na.rm=TRUE))%>%
                        filter(total_consumption==0)

ZeroTotalConsumption <- ZeroTotalConsumption %>% filter(!service_point_sn %in% family_vacant_servicepoint$service_point_sn)

## Detect zero consumptions or NA for > 21 days, from today’s date
ZeroConsumption_21days <- consumption_DB %>%
                          arrange(service_point_sn, date_consumption) %>%
                          group_by(service_point_sn)%>%
                          mutate(zero = ifelse(interpolated_consumption==0, 1, 0),
                          zerocount = zero * ave(zero, c(0L, cumsum(diff(zero) != 0)), FUN = seq_along))

CountData <- ZeroConsumption_21days %>%
             arrange(service_point_sn, date_consumption) %>%
             group_by(service_point_sn) %>%
             mutate(daycount = floor(zerocount/24))

MaxCountData_Ref_LatestDate <- CountData %>%
                               group_by(service_point_sn) %>%
                               filter(date_consumption==max(date_consumption))%>%
                               dplyr::summarise(maxdaycount = max(daycount)) %>%
                               filter(maxdaycount >= 21) %>%
                               filter(!service_point_sn %in% family_vacant_servicepoint$service_point_sn) %>%
                               filter(!service_point_sn %in% ZeroTotalConsumption$service_point_sn)

## Detect zero consumptiom or NA on compoundmeters (sum of main+bypass) for > 7 days
ZeroConsumption_7days <- consumption_DB %>%
                         filter(grepl("M|B",service_point_sn)) %>%
                         arrange(service_point_sn, date_consumption) %>%
                         group_by(service_point_sn)%>%
                         mutate(zero = ifelse(interpolated_consumption==0, 1, 0),
                         zerocount = zero * ave(zero, c(0L, cumsum(diff(zero) != 0)), FUN = seq_along))

CountData_MB <- ZeroConsumption_7days %>%
                arrange(service_point_sn, date_consumption) %>%
                group_by(service_point_sn) %>%
                mutate(daycount = floor(zerocount/24))

MaxCountData_Ref_LatestDate_MB <- CountData_MB %>%
                                  group_by(service_point_sn) %>%
                                  filter(date_consumption==max(date_consumption))%>%
                                  dplyr::summarise(maxdaycount = max(daycount)) %>%
                                  filter(maxdaycount >= 7) %>%
                                  filter(!service_point_sn %in% family_vacant_servicepoint$service_point_sn) %>%
                                  filter(!service_point_sn %in% ZeroTotalConsumption$service_point_sn) %>%
                                  filter(!service_point_sn %in% MaxCountData_Ref_LatestDate$service_point_sn)

ZeroConsumptionChecks = data.table(ItemDescription=c(rep("Zero_InterpolatedConsumption",length(ZeroTotalConsumption$service_point_sn)),
                                                     rep("Morethan21days_ZeroConsumption",length(MaxCountData_Ref_LatestDate$service_point_sn)),
                                                     rep("Morethan7days_MainByPass_ZeroConsumption",length(MaxCountData_Ref_LatestDate_MB$service_point_sn))),
                                   ServicePointSn=c(ZeroTotalConsumption$service_point_sn,
                                                    MaxCountData_Ref_LatestDate$service_point_sn,
                                                    MaxCountData_Ref_LatestDate_MB$service_point_sn)) 

ZeroConsumptionChecks <- servicepoint[match(ZeroConsumptionChecks$ServicePointSn,servicepoint$service_point_sn),] %>%
                         dplyr::select_("service_point_sn","block","floor","site")
row.names(ZeroConsumptionChecks) <- NULL

save(ZeroConsumptionChecks,file="/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionChecks.RData")
write.csv(ZeroConsumptionChecks,"/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionChecks.csv",row.names=FALSE)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ZeroConsumptionChecks successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)