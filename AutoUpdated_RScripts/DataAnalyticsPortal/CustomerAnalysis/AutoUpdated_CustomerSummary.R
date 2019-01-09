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

customer <- as.data.frame(tbl(con_amrcms,"customer"))
customer_points <- as.data.frame(tbl(con_amrcms,"customer_points"))
customer_family <- inner_join(customer,family,by=c("id_family"="id"))
customer_family_PG <- customer_family %>% dplyr::filter(substr(address,1,2)=="PG")
customer_family_YH <- customer_family %>% dplyr::filter(substr(address,1,2)=="YH")
customer_points_PG <- customer_points %>% dplyr::filter(customer_id %in% customer_family_PG$id)
customer_points_YH <- customer_points %>% dplyr::filter(customer_id %in% customer_family_YH$id)

family_servicepoint_Customer <- inner_join(family_servicepoint,customer,by=c("id"="id_family"))
family_servicepoint_Customer_PG <- family_servicepoint_Customer %>% dplyr::filter(substr(block,1,2)=="PG")
family_servicepoint_Customer_YH <- family_servicepoint_Customer %>% dplyr::filter(substr(block,1,2)=="YH")
      
CustomerAgeRange_PG <- as.data.frame(tbl(con_amrcms,"customer")) %>% 
  dplyr::filter(username %in% family_servicepoint_Customer_PG$username & email %in% family_servicepoint_Customer_PG$email) %>%
  dplyr::select_("id_family","age_range") %>%
  dplyr::group_by(age_range) %>%
  dplyr::summarise(Count=n())
CustomerAgeRange_PG$Percent <- round(prop.table(CustomerAgeRange_PG$Count)*100,1)
write.csv(CustomerAgeRange_PG,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAgeRange_PG.csv")

CustomerAgeRange_YH <- as.data.frame(tbl(con_amrcms,"customer")) %>% 
  dplyr::filter(username %in% family_servicepoint_Customer_YH$username & email %in% family_servicepoint_Customer_YH$email) %>%
  dplyr::select_("id_family","age_range") %>%
  dplyr::group_by(age_range) %>%
  dplyr::summarise(Count=n())
CustomerAgeRange_YH$Percent <- round(prop.table(CustomerAgeRange_YH$Count)*100,1)
write.csv(CustomerAgeRange_YH,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAgeRange_YH.csv")

# CustomerAgeRange <- function(site){
#   CustomerAgeRange <- as.data.frame(tbl(con_amrcms,"customer")) %>% 
#     dplyr::filter(username %in% family_servicepoint_Customer_PG$username & email %in% family_servicepoint_Customer_PG$email) %>%
#     dplyr::select_("id_family","age_range") %>%
#     dplyr::group_by(age_range) %>%
#     dplyr::summarise(Count=n())
#   CustomerAgeRange$Percent <- round(prop.table(CustomerAgeRange$Count)*100,1)
#   write.csv(CustomerAgeRange,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAgeRange.csv")
# }

# pie chart of customers per log-in frequency : unique, low, moderate, regular
# (amr_cms : customer_points table, fields: reward category=daily login, created_date=login-date)
# Unique = 1 login so far
# Low : at least or equal to once a month (>=1/30) & less than twice a month (<2/30) 
# Moderate : at least twice a month (>=2/30) & less than once per week (<4/30)
# Regular : at least once a week (>=4/30)
  
# Unique is 1 login ever.
# Low is > 1 login ever AND < 2/30

customer_points_daily_login_PG <- customer_points_PG %>%
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

Unique_Customers <- length(unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Unique")]))
Low_Customers <- length(unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Low")]))
Moderate_Customers <- length(unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Moderate")]))
Regular_Customers <- length(unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Regular")]))
Total_Customers <- sum(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers)

UniqueCustomers <- unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Unique")])
LowCustomers <- unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Low")])
ModerateCustomers <- unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Moderate")])
RegularCustomers <- unique(customer_points_daily_login_PG$customer_id[which(customer_points_daily_login_PG$Category=="Regular")])

CustomersLoginFrequency_PG <- data.frame(CustomerID=c(UniqueCustomers,LowCustomers,ModerateCustomers,RegularCustomers),
                                      Category=c(rep("Unique",Unique_Customers),rep("Low",Low_Customers),rep("Moderate",Moderate_Customers),
                                                 rep("Regular",Regular_Customers)))

write.csv(CustomersLoginFrequency_PG,"/srv/shiny-server/DataAnalyticsPortal/data/CustomersLoginFrequency_PG.csv")

CustomersCategory <- data.frame(Category=c("Unique","Low","Moderate","Regular"),
                                Total=c(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers))
Category_Percent <- round(prop.table(CustomersCategory$Total)*100,2)
CategoryUnique <- paste("Unique ","(",Unique_Customers,")",sep="")
CategoryLow <- paste("Low ","(",Low_Customers,")",sep="")
CategoryModerate <- paste("Moderate ","(",Moderate_Customers,")",sep="")
CategoryRegular <- paste("Regular ","(",Regular_Customers,")",sep="")

CustomerLoginFrequency_Percent_PG <- data.frame(Category=c(CategoryUnique,CategoryLow,CategoryModerate,CategoryRegular),
                                             Category_Percent)

write.csv(CustomerLoginFrequency_Percent_PG,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerLoginFrequency_PG.csv")

customer_points_daily_login_YH <- customer_points_YH %>%
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

Unique_Customers <- length(unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Unique")]))
Low_Customers <- length(unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Low")]))
Moderate_Customers <- length(unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Moderate")]))
Regular_Customers <- length(unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Regular")]))
Total_Customers <- sum(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers)

UniqueCustomers <- unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Unique")])
LowCustomers <- unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Low")])
ModerateCustomers <- unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Moderate")])
RegularCustomers <- unique(customer_points_daily_login_YH$customer_id[which(customer_points_daily_login_YH$Category=="Regular")])

CustomersLoginFrequency_YH <- data.frame(CustomerID=c(UniqueCustomers,LowCustomers,ModerateCustomers,RegularCustomers),
                                         Category=c(rep("Unique",Unique_Customers),rep("Low",Low_Customers),rep("Moderate",Moderate_Customers),
                                                    rep("Regular",Regular_Customers)))

write.csv(CustomersLoginFrequency_YH,"/srv/shiny-server/DataAnalyticsPortal/data/CustomersLoginFrequency_YH.csv")

CustomersCategory <- data.frame(Category=c("Unique","Low","Moderate","Regular"),
                                Total=c(Unique_Customers,Low_Customers,Moderate_Customers,Regular_Customers))
Category_Percent <- round(prop.table(CustomersCategory$Total)*100,2)
CategoryUnique <- paste("Unique ","(",Unique_Customers,")",sep="")
CategoryLow <- paste("Low ","(",Low_Customers,")",sep="")
CategoryModerate <- paste("Moderate ","(",Moderate_Customers,")",sep="")
CategoryRegular <- paste("Regular ","(",Regular_Customers,")",sep="")

CustomerLoginFrequency_Percent_YH <- data.frame(Category=c(CategoryUnique,CategoryLow,CategoryModerate,CategoryRegular),
                                                Category_Percent)

write.csv(CustomerLoginFrequency_Percent_YH,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerLoginFrequency_YH.csv")

VisitorsPerDay_PG <- customer_points_daily_login_PG %>% dplyr::group_by(Category,Date) %>%
                     dplyr::summarise(Connections=n())

VisitorsPerDayPG_Wide <- spread(VisitorsPerDay_PG,Category,Connections) 
# replace NA with zeros
VisitorsPerDayPG_Wide[is.na(VisitorsPerDayPG_Wide )] <- 0 

VisitorsPerDayPG_Wide <- VisitorsPerDayPG_Wide %>% dplyr::mutate(Total=Unique+Low+Moderate+Regular)

write.csv(VisitorsPerDayPG_Wide,"/srv/shiny-server/DataAnalyticsPortal/data/VisitorsPerDay_PG.csv")

VisitorsPerDay_YH <- customer_points_daily_login_YH %>% dplyr::group_by(Category,Date) %>%
  dplyr::summarise(Connections=n())

VisitorsPerDayYH_Wide <- spread(VisitorsPerDay_YH,Category,Connections) 
# replace NA with zeros
VisitorsPerDayYH_Wide[is.na(VisitorsPerDayYH_Wide )] <- 0 

VisitorsPerDayYH_Wide <- VisitorsPerDayYH_Wide %>% dplyr::mutate(Total=Unique+Low+Moderate+Regular)

write.csv(VisitorsPerDayYH_Wide,"/srv/shiny-server/DataAnalyticsPortal/data/VisitorsPerDay_YH.csv")

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

CustomerAccumulativePoints_PG <- customer_points_PG %>%
  dplyr::mutate(Date=date(created_date),Time=substr(created_date,12,19)) %>%
  dplyr::select_("customer_id","Date","points") %>%
  dplyr::group_by(customer_id) %>%
  dplyr::mutate(AccumulativePoints=cumsum(points))

write.csv(CustomerAccumulativePoints_PG,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulativePoints_PG.csv")

CustomerAccumulativePoints_YH <- customer_points_YH %>%
  dplyr::mutate(Date=date(created_date),Time=substr(created_date,12,19)) %>%
  dplyr::select_("customer_id","Date","points") %>%
  dplyr::group_by(customer_id) %>%
  dplyr::mutate(AccumulativePoints=cumsum(points))

write.csv(CustomerAccumulativePoints_YH,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulativePoints_YH.csv")

CustomerAccumulatedPoints_Users_PG <- CustomerAccumulativePoints_PG %>% 
  dplyr::group_by(customer_id) %>%
  dplyr::filter(AccumulativePoints==max(AccumulativePoints))

h <- hist(CustomerAccumulatedPoints_Users_PG$AccumulativePoints)
x = h$mids
y = h$counts
CustomerAccumulatedPoints_DF_PG <- data.frame(AccumulativePoints=x,NumberOfUsers=y)
write.csv(CustomerAccumulatedPoints_DF_PG,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulatedPoints_PG.csv")

CustomerAccumulatedPoints_Users_YH <- CustomerAccumulativePoints_YH %>% 
  dplyr::group_by(customer_id) %>%
  dplyr::filter(AccumulativePoints==max(AccumulativePoints))

h <- hist(CustomerAccumulatedPoints_Users_YH$AccumulativePoints)
x = h$mids
y = h$counts
CustomerAccumulatedPoints_DF_YH <- data.frame(AccumulativePoints=x,NumberOfUsers=y)
write.csv(CustomerAccumulatedPoints_DF_YH,"/srv/shiny-server/DataAnalyticsPortal/data/CustomerAccumulatedPoints_YH.csv")

Updated_DateTime_CustomerSummary <- paste("Last Updated on ",now(),"."," Next Update on ",now()+24*60*60,".",sep="")

save(CustomerAgeRange_PG,CustomerAgeRange_YH,
     CustomerLoginFrequency_Percent_PG,CustomerLoginFrequency_Percent_YH,
     VisitorsPerDayPG_Wide,VisitorsPerDayYH_Wide,
     BlockOnlineOffline,CustomerSignUp,
     CustomerAccumulativePoints_PG,CustomerAccumulativePoints_YH,
     CustomerAccumulatedPoints_Users_PG,CustomerAccumulatedPoints_Users_YH,
     Updated_DateTime_CustomerSummary,
     file="/srv/shiny-server/DataAnalyticsPortal/data/CustomerSummary.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerSummary successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)