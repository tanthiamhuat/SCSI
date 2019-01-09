rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,xts)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

weekly_consumption <- as.data.frame(tbl(con,"weekly_consumption"))

load("/srv/shiny-server/DataAnalyticsPortal/Profiling_V2/Output/04-Final_Profile.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

# Water savings per group and per week.
# (groups are determined in customer segmentation)

Customers <- Y %>% 
             dplyr::rename(Group=Grp,Level=Lev) %>%
             dplyr::mutate(AverageDailyConsumption=round(adc*1000)) %>%
             dplyr::select_("service_point_sn","block","room_type","AverageDailyConsumption","Group","Level","Profile")

Customers_Savings <- weekly_consumption %>% dplyr::filter(!is.na(water_savings)) %>%
                     dplyr::select_("service_point_sn","week_number","water_savings") 

Customers_Savings_Profile <- inner_join(Customers_Savings,Customers,by="service_point_sn") %>%
                             dplyr::select_("service_point_sn","block","room_type","week_number","water_savings","Profile") %>%
                             group_by(week_number,Profile) %>%
                             dplyr::summarise(WaterSavings=sum(water_savings))

Customers_Savings_Profile_Weekly <- inner_join(Customers_Savings_Profile,Week.date,by=c("week_number"="week")) %>%
                                    dplyr::select_("Profile","WaterSavings","end") %>%
                                    dplyr::rename(Date=end) %>%
                                    dplyr::filter(Date >="2017-02-19")
Customers_Savings_Profile_Weekly[1] <- NULL

Customers_Savings_Profile_Weekly_wide <- dcast(Customers_Savings_Profile_Weekly, Date ~ Profile, value.var="WaterSavings")

Customers_Savings_Profile_Weekly_wide$Date <- strptime(Customers_Savings_Profile_Weekly_wide[,1],format="%Y-%m-%d")
CustomerProfileWaterSavings_Weekly <- xts(Customers_Savings_Profile_Weekly_wide[,-1], order.by=Customers_Savings_Profile_Weekly_wide[,1])

Updated_DateTime_CustomerProfileWaterSavings <- paste("Last Updated on ",now(),"."," Next Update on ",now()+7*24*60*60,".",sep="")

save(CustomerProfileWaterSavings_Weekly,Updated_DateTime_CustomerProfileWaterSavings,
     file="/srv/shiny-server/DataAnalyticsPortal/data/CustomerProfileWaterSavings_Weekly.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_CustomerProfileWaterSavings successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
 
