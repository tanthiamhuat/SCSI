family_ACTIVE <- as.data.frame(tbl(con,"family") %>% 
                                 dplyr::filter(pub_cust_id!="EMPTY" & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & 
                                                 status=="ACTIVE" & id_service_point!=601)) # exclude AHL
servicepoint <- as.data.frame(tbl(con,"service_point")) 
family_servicepoint <- inner_join(family_ACTIVE,servicepoint,by=c("id_service_point"="id","room_type"))

Punggol_lastyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")

PunggolConsumption_SUB <- Punggol_lastyear %>%
  dplyr::filter(!(room_type %in% c("NIL","HDBCD")) & service_point_sn !="3100660792") %>%  # remove AHL
  select(service_point_sn,block,room_type,floor,adjusted_consumption,adjusted_date) %>%
  arrange(adjusted_date)

PunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block","floor","room_type")) %>%
  group_by(service_point_sn) %>%
  dplyr::filter(date(adjusted_date)>=date(move_in_date) & (date(adjusted_date)<date(move_out_date) | is.na(move_out_date)))

PunggolConsumption <- unique(PunggolConsumption[c("service_point_sn","adjusted_consumption","adjusted_date","num_house_member","room_type","block")])

DailyConsumption <- PunggolConsumption %>%
  dplyr::filter(!is.na(adjusted_consumption)) %>%
  dplyr::mutate(Year=year(adjusted_date),date=date(adjusted_date),Month=month(adjusted_date)) %>%
  group_by(service_point_sn,Year,Month,date,room_type,num_house_member,block) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption,na.rm = TRUE)) 

BlockDailyConsumption <- DailyConsumption %>% dplyr::group_by(date,block) %>%
                         dplyr::summarise(TotalDailyConsumption=sum(DailyConsumption,na.rm = TRUE))

BlockDailyConsumption_Wide_2016 <- spread(BlockDailyConsumption,block,TotalDailyConsumption)
write.csv(BlockDailyConsumption_Wide_2016,file="/srv/shiny-server/DataAnalyticsPortal/data/DailyBlockConsumption_2017.csv")
