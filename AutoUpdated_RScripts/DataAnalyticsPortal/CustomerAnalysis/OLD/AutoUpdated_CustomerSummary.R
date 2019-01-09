## PLots: a) Pie-Chart of Customer Age b) Online/Offline Household
##        c) Pie-Chart of Customer Login Frequency d) Cumulative Sum of Online Customers SignUp

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

load("/srv/shiny-server/DataAnalyticsPortal/data/Dashboard.RData")

today <- today()

x=Sys.info()
if (x[which(names(x)=='user')]=="thiamhuat")
{
  con_amrcms <- src_postgres(host = "52.77.188.178", user = "thiamhuat", 
                             password = "thiamhuat1234##", dbname="proddb",
                             options="-c search_path=amr_cms")
  
} else if (x[which(names(x)=='user')]=="dapuser") {
  con_amrcms <- src_postgres(host = "10.206.13.72", user = "dapuser", 
                             password = "dapuser1234##", dbname="amrstaging",
                             options="-c search_path=amr_cms")
}

family <- as.data.frame(tbl(con,"family") %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" 
                         & !(room_type %in% c("MAIN","BYPASS","HDBCD")) & id_service_point!="601"))
servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(!service_point_sn %in% c("3100507837M","3100507837B","3100660792")))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type")) 

CustomerTable <- as.data.frame(tbl(con_amrcms,"customer"))
CustomerPointsTable <- as.data.frame(tbl(con_amrcms,"customer_points"))
family_servicepoint_Customer <- inner_join(family_servicepoint,CustomerTable,by=c("id"="id_family"))
      
CustomerAgeRange <- as.data.frame(tbl(con_amrcms,"customer")) %>% 
  dplyr::select_("id_family","age_range") %>%
  dplyr::group_by(age_range) %>%
  dplyr::summarise(Count=n())
CustomerAgeRange$Percent <- round(prop.table(CustomerAgeRange$Count)*100,1)

write.csv(CustomerAgeRange,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAgeRange.csv")

# pie chart of customers per log-in frequency : unique, low, moderate, regular
# (amr_cms : customer_points table, fields: reward category=daily login, created_date=login-date)
# Unique = 1 login so far
# Low : at least or equal to once a month (>=1/30) & less than twice a month (<2/30) 
# Moderate : at least twice a month (>=2/30) & less than once per week (<4/30)
# Regular : at least once a week (>=4/30)
  
# customer_points_daily_login <- as.data.frame(tbl(con_amrcms,"customer_points")) %>%
#   dplyr::filter(reward_category=="daily_login") %>%
#   dplyr::mutate(Date=date(created_date)) %>%
#   dplyr::group_by(customer_id) %>%
#   dplyr::select_("customer_id","points","reward_category","Date") %>%
#   dplyr::mutate(FirstDate=min(Date),DaysDiff=as.integer(difftime(today,FirstDate,units="days"))) %>%
#   dplyr::mutate(Count=n()-1,Ratio=Count/DaysDiff) %>% as.data.frame() %>%
#   dplyr::mutate(Category=ifelse(Ratio >=0 & Ratio < 0.033,"Unique",
#                          ifelse(Ratio >= 0.033 & Ratio < 0.066,"Low",
#                          ifelse(Ratio >=0.066 & Ratio < 0.13,"Moderate",
#                          ifelse(Ratio >=0.13 & Ratio <=1,"Regular",0)))))

# Unique is 1 login ever.
# Low is > 1 login ever AND < 2/30

customer_points_daily_login <- as.data.frame(tbl(con_amrcms,"customer_points")) %>%
  dplyr::filter(reward_category=="daily_login") %>%
  dplyr::mutate(Date=date(created_date)) %>%
  dplyr::group_by(customer_id) %>%
  dplyr::select_("customer_id","points","reward_category","Date") %>%
  dplyr::mutate(FirstDate=min(Date),DaysDiff=as.integer(difftime(today,FirstDate,units="days"))) %>%
  dplyr::mutate(Count=n(),Ratio=(Count-1)/DaysDiff) %>%
  dplyr::mutate(Category=ifelse(Count==1,"Unique",
                         ifelse(Count >1 & Ratio <= 0.067,"Low",
                         ifelse(Ratio > 0.067 & Ratio < 0.13,"Moderate",
                         ifelse(Ratio >=0.13 & Ratio <=1,"Regular",0)))))

Unique_Customers <- length(unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Unique")]))
Low_Customers <- length(unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Low")]))
Moderate_Customers <- length(unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Moderate")]))
Regular_Customers <- length(unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Regular")]))
Total_Customers <- sum(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers)

UniqueCustomers <- unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Unique")])
LowCustomers <- unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Low")])
ModerateCustomers <- unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Moderate")])
RegularCustomers <- unique(customer_points_daily_login$customer_id[which(customer_points_daily_login$Category=="Regular")])

CustomersLoginFrequency <- data.frame(CustomerID=c(UniqueCustomers,LowCustomers,ModerateCustomers,RegularCustomers),
                                      Category=c(rep("Unique",Unique_Customers),rep("Low",Low_Customers),rep("Moderate",Moderate_Customers),
                                                 rep("Regular",Regular_Customers)))

write.csv(CustomersLoginFrequency,"/srv/shiny-server/DataAnalyticsPortal/data/CustomersLoginFrequency.csv")

CustomersCategory <- data.frame(Category=c("Unique","Low","Moderate","Regular"),
                                Total=c(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers))
Category_Percent <- round(prop.table(CustomersCategory$Total)*100,2)
CategoryUnique <- paste("Unique ","(",Unique_Customers,")",sep="")
CategoryLow <- paste("Low ","(",Low_Customers,")",sep="")
CategoryModerate <- paste("Moderate ","(",Moderate_Customers,")",sep="")
CategoryRegular <- paste("Regular ","(",Regular_Customers,")",sep="")

CustomerLoginFrequency_Percent <- data.frame(Category=c(CategoryUnique,CategoryLow,CategoryModerate,CategoryRegular),
                                             Category_Percent)

write.csv(CustomerLoginFrequency_Percent,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerLoginFrequency.csv")

VisitorsPerDay <- customer_points_daily_login %>% dplyr::group_by(Category,Date) %>%
  dplyr::summarise(Connections=n())

VisitorsPerDay_Wide <- spread(VisitorsPerDay,Category,Connections) 
# replace NA with zeros
VisitorsPerDay_Wide[is.na(VisitorsPerDay_Wide )] <- 0 

VisitorsPerDay_Wide <- VisitorsPerDay_Wide %>% dplyr::mutate(Total=Unique+Low+Moderate+Regular)

write.csv(VisitorsPerDay_Wide,"/srv/shiny-server/DataAnalyticsPortal/data/VisitorsPerDay.csv")

OnlineOffline_HH <- family %>% dplyr::select_("address","online_status") %>%
  dplyr::mutate(block=substr(address,1,5)) %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(Online=sum(online_status=="ACTIVE"),Offline=sum(online_status=="INACTIVE"))

y <- OnlineOffline_HH$block
block_online <- OnlineOffline_HH$Online
block_offline <- OnlineOffline_HH$Offline
BlockOnlineOffline <- data.frame(y, block_online, block_offline) %>%
  dplyr::mutate(block_online_percent=round(block_online/(block_online+block_offline)*100,1),
                block_offline_percent=round(block_offline/(block_online+block_offline)*100,1))

write.csv(BlockOnlineOffline,"/srv/shiny-server/DataAnalyticsPortal/data/BlockOnlineOffline.csv")

## Online Customer SignUp
total_HH <- as.numeric(OverallQuantities$Punggol[which(OverallQuantities$ItemDescription=="Qty Occupied Household")])-
  as.numeric(OverallQuantities$Punggol[which(OverallQuantities$ItemDescription=="Qty Occupied HH (zero consumption)")])

CustomerSignUp <- as.data.frame(tbl(con_amrcms,"customer")) %>% 
  dplyr::filter(is_main=="TRUE") %>%
  dplyr::select_("id_family","created_at") %>%
  dplyr::mutate(Date=date(created_at)) %>%
  dplyr::group_by(Date) %>%
  dplyr::summarise(Count=n()) %>%
  dplyr::mutate(SignUp=cumsum(Count),PerCent=round(SignUp/total_HH*100)) 

write.csv(CustomerSignUp,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerSignUp.csv")

CustomerAccumulativePoints <- as.data.frame(tbl(con_amrcms,"customer_points")) %>%
  dplyr::mutate(Date=date(created_date),Time=substr(created_date,12,19)) %>%
  dplyr::select_("customer_id","Date","points") %>%
  dplyr::group_by(customer_id) %>%
  dplyr::mutate(AccumulativePoints=cumsum(points))

write.csv(CustomerAccumulativePoints,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulativePoints.csv")

CustomerAccumulatedPoints_Users <- CustomerAccumulativePoints %>% 
                                   dplyr::group_by(customer_id) %>%
                                   dplyr::filter(AccumulativePoints==max(AccumulativePoints))

h <- hist(CustomerAccumulatedPoints_Users$AccumulativePoints)
x = h$mids
y = h$counts
CustomerAccumulatedPoints_DF <- data.frame(AccumulativePoints=x,NumberOfUsers=y)
write.csv(CustomerAccumulatedPoints_DF,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulatedPoints.csv")

Updated_DateTime_CustomerSummary <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(CustomerAgeRange,
     CustomerLoginFrequency_Percent,VisitorsPerDay_Wide,
     BlockOnlineOffline,CustomerSignUp,
     CustomerAccumulativePoints,CustomerAccumulatedPoints_Users,
     Updated_DateTime_CustomerSummary,
     file="/srv/shiny-server/DataAnalyticsPortal/data/CustomerSummary.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerSummary successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)