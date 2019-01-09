## pie chart is for monthly data

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

con_amrcms <- src_postgres(host = "52.77.188.178", user = "thiamhuat", 
                           password = "thiamhuat1234##", dbname="amrstaging",
                           options="-c search_path=amr_cms")

proddb <- dbConnect(PostgreSQL(), dbname="proddb",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

usage_analysis <- as.data.frame(tbl(con_amrcms,"usage_analysis")) %>% dplyr::filter(no_of_time!=0)
activity_category <- as.data.frame(tbl(con_amrcms,"activity_category"))

usage_analysis_activity <- inner_join(usage_analysis,activity_category,by=c("activity_type_id"="id"))

usage_analysis_activity_familyservicepoint <- inner_join(usage_analysis_activity,family_servicepoint, 
                                                         by=c("family_id"="id")) %>%
  dplyr::select_("family_id","id_service_point","consumption_id","pub_cust_id","service_point_sn","address.x","activity_type","date_consumption")

consumption_2018 <- as.data.table(tbl(proddb,"consumption"))[date_consumption>="2018-01-01"]
save(consumption_2018,file="/srv/shiny-server/DataAnalyticsPortal/data/consumption_2018.RData")

usage_analysis_activity_familyservicepoint_2018 <- usage_analysis_activity_familyservicepoint %>% dplyr::filter(date_consumption>="2018-01-01")

# https://stackoverflow.com/questions/40515869/rounding-percentages-to-100-in-r
round_percent <- function(x) { 
  x <- x/sum(x)*100  # Standardize result
  res <- floor(x)    # Find integer bits
  rsum <- sum(res)   # Find out how much we are missing
  if(rsum<100) { 
    # Distribute points based on remainders and a random tie breaker
    o <- order(x%%1, sample(length(x)), decreasing=TRUE) 
    res[o[1:(100-rsum)]] <- res[o[1:(100-rsum)]]+1
  } 
  res 
}

usage_breakdown <- inner_join(usage_analysis_activity_familyservicepoint_2018,consumption_2018,by=c("id_service_point","consumption_id"="id")) %>%
                   dplyr::select_("family_id","pub_cust_id","activity_type","adjusted_consumption") %>%
                   dplyr::group_by(family_id,pub_cust_id,activity_type) %>%
                   dplyr::summarise(TotalConsumption=sum(adjusted_consumption)) %>%
                   dplyr::mutate(SumTotalConsumption=sum(TotalConsumption),Percent=round_percent(TotalConsumption/SumTotalConsumption*100))
usage_breakdown$TotalConsumption <- NULL
usage_breakdown$SumTotalConsumption <- NULL

UsageBreakdown_wide <- spread(usage_breakdown,activity_type,Percent) %>% as.data.frame()
UsageBreakdown_wide$id <- seq(1,nrow(UsageBreakdown_wide))

colnames(UsageBreakdown_wide)[which(names(UsageBreakdown_wide) == "Cooking")] <- "cooking_usage"
colnames(UsageBreakdown_wide)[which(names(UsageBreakdown_wide) == "Laundry")] <- "laundry_usage"
colnames(UsageBreakdown_wide)[which(names(UsageBreakdown_wide) == "Shower")] <- "shower_usage"
colnames(UsageBreakdown_wide)[which(names(UsageBreakdown_wide) == "Washing")] <- "cleaning_usage"
colnames(UsageBreakdown_wide)[which(names(UsageBreakdown_wide) == "Others")] <- "others_usage"

UsageBreakdown_wide$date_created <- today()
UsageBreakdown_wide <- UsageBreakdown_wide[c("id", "family_id", "pub_cust_id","shower_usage","cooking_usage","laundry_usage","cleaning_usage","others_usage","date_created")]

dbWriteTable(mydb, "usage_breakdown", UsageBreakdown_wide, append=FALSE, row.names=F, overwrite=TRUE) # append table
dbWriteTable(proddb, "usage_breakdown", UsageBreakdown_wide, append=FALSE, row.names=F, overwrite=TRUE) # append table
