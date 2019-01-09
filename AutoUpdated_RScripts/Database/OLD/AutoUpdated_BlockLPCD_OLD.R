# Different groups of customers in each block (Near-Zero, Efficient, Average, Extreme-High)
#	My Average Neighbours (average lpcd of the average group) change every month (last month) 
# to take into account monthly occupancy of each customer.

#	My Efficient Neighbours (average lpcd of the efficient group of my block) change every month (last month) 
# to take into account monthly occupancy of each customer.

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()
if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,RPostgreSQL,lubridate)

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
servicepoint <- as.data.frame(tbl(con,"service_point"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")

PunggolConsumption_SUB <- Punggol_All %>%
  dplyr::filter(!(room_type %in% c("NIL")) & !(is.na(room_type))) %>%
  dplyr::mutate(day=D,month=M) %>%                  
  select(service_point_sn,block,room_type,floor,adjusted_consumption,date_consumption,day,month) %>%
  arrange(date_consumption)

lastmonth <- month(floor_date(Sys.Date() - months(c(1)), "month"))

DailyPunggolConsumption <- inner_join(PunggolConsumption_SUB,family_servicepoint,by=c("service_point_sn","block")) %>%
  dplyr::mutate(date=date(date_consumption),wd=weekdays(date_consumption)) %>%
  group_by(service_point_sn,block,date,month,day,wd) %>%
  dplyr::summarise(DailyConsumption=sum(adjusted_consumption)) %>%
  dplyr::filter(service_point_sn !="3101127564") # exclude ChildCare 

X1 <- DailyPunggolConsumption %>% group_by(service_point_sn,block) %>%
  filter(month==lastmonth) %>%
  dplyr::summarise(ConsumptionPerMonth=sum(DailyConsumption))

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy"))
monthly_occupancy_extracted <- monthly_occupancy %>% 
     filter(month(lastupdated)==lastmonth & service_point_sn!="3101127564") 
# exclude ChildCare

X1_occupancy_days <- inner_join(X1,monthly_occupancy_extracted,by="service_point_sn")
X1_occupancy_days_family_servicepoint <- inner_join(X1_occupancy_days,family_servicepoint,by=c("service_point_sn","block"))

X1 <- X1_occupancy_days_family_servicepoint %>% 
      mutate(my_average_lpcd=ifelse(ConsumptionPerMonth==0,0,
                             ifelse(ConsumptionPerMonth!=0,
                                    round(ConsumptionPerMonth/(num_house_member*occupancy_days)),0)))

# Different groups of customers in each block (Near-Zero, Efficient, Average, ExtremeHigh)
res <- hist(X1$my_average_lpcd)
group_NearZero <-res$breaks[1:3]
group_Efficient <-res$breaks[3:4]
group_Average <- res$breaks[4:6]
group_ExtremeHigh <-res$breaks[6]

X1 <- X1 %>% mutate(Group=ifelse(my_average_lpcd < 100,"NearZero",
                          ifelse(my_average_lpcd >= 100 & 
                                 my_average_lpcd <= 110,"Efficient",
                          ifelse(my_average_lpcd > 110 &
                                 my_average_lpcd < 150,"Average",
                          ifelse(my_average_lpcd >= 150,"ExtremeHigh",0)))))

Block_Average_LPCD <- X1 %>% group_by(block,Group) %>% 
  filter(Group=="Average") %>%
  dplyr::summarize(average_block_lpcd=round(mean(my_average_lpcd)))

Block_Efficient_LPCD <- X1 %>% group_by(block,Group) %>% 
  filter(Group=="Efficient") %>%
  dplyr::summarize(efficient_block_lpcd=round(mean(my_average_lpcd)))

BlockLPCD <- as.data.frame(cbind(Block_Average_LPCD,Block_Efficient_LPCD)[,c(1,3,6)])
date_created <- data.frame(rep(today(),nrow(BlockLPCD)))
colnames(date_created) <- "date_created"
BlockLPCD <- cbind(BlockLPCD,date_created)
BlockLPCD$site <- "Punggol"

BlockLPCD$id <- as.integer(rownames(BlockLPCD))
BlockLPCD <- BlockLPCD %>% dplyr::select_("id","block","average_block_lpcd","efficient_block_lpcd","date_created","site")

dbSendQuery(mydb, "delete from block_lpcd")
dbWriteTable(mydb, "block_lpcd", BlockLPCD, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockLPCD successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
