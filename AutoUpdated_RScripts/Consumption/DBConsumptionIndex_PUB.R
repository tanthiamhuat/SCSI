rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

load("/srv/shiny-server/DataAnalyticsPortal/data/ConsumptionIndexDownload_PUB.RData")

Consumption_2016 <- ConsumptionDownload %>% filter(year(date)==2016) %>% select(-week)
Index_2016 <- IndexDownload %>% filter(year(date)==2016) %>% select(-week)

Consumption_2016_Apr <- Consumption_2016 %>% filter(month(date)==4)
Consumption_2016_May <- Consumption_2016 %>% filter(month(date)==5)
Consumption_2016_Jun <- Consumption_2016 %>% filter(month(date)==6)
Consumption_2016_Jul <- Consumption_2016 %>% filter(month(date)==7)
Consumption_2016_Aug <- Consumption_2016 %>% filter(month(date)==8)
Consumption_2016_Sep <- Consumption_2016 %>% filter(month(date)==9)
Consumption_2016_Oct <- Consumption_2016 %>% filter(month(date)==10)
Consumption_2016_Nov <- Consumption_2016 %>% filter(month(date)==11)
Consumption_2016_Dec <- Consumption_2016 %>% filter(month(date)==12)

Index_2016_Apr <- Index_2016 %>% filter(month(date)==4)
Index_2016_May <- Index_2016 %>% filter(month(date)==5)
Index_2016_Jun <- Index_2016 %>% filter(month(date)==6)
Index_2016_Jul <- Index_2016 %>% filter(month(date)==7)
Index_2016_Aug <- Index_2016 %>% filter(month(date)==8)
Index_2016_Sep <- Index_2016 %>% filter(month(date)==9)
Index_2016_Oct <- Index_2016 %>% filter(month(date)==10)
Index_2016_Nov <- Index_2016 %>% filter(month(date)==11)
Index_2016_Dec <- Index_2016 %>% filter(month(date)==12)

write.csv(Consumption_2016_Apr,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Apr.csv",row.names=FALSE)
write.csv(Consumption_2016_May,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_May.csv",row.names=FALSE)
write.csv(Consumption_2016_Jun,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Jun.csv",row.names=FALSE)
write.csv(Consumption_2016_Jul,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Jul.csv",row.names=FALSE)
write.csv(Consumption_2016_Aug,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Aug.csv",row.names=FALSE)
write.csv(Consumption_2016_Sep,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Sep.csv",row.names=FALSE)
write.csv(Consumption_2016_Oct,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Oct.csv",row.names=FALSE)
write.csv(Consumption_2016_Nov,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Nov.csv",row.names=FALSE)
write.csv(Consumption_2016_Dec,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Consumption_2016_Dec.csv",row.names=FALSE)

write.csv(Index_2016_Apr,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Apr.csv",row.names=FALSE)
write.csv(Index_2016_May,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_May.csv",row.names=FALSE)
write.csv(Index_2016_Jun,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Jun.csv",row.names=FALSE)
write.csv(Index_2016_Jul,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Jul.csv",row.names=FALSE)
write.csv(Index_2016_Aug,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Aug.csv",row.names=FALSE)
write.csv(Index_2016_Sep,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Sep.csv",row.names=FALSE)
write.csv(Index_2016_Oct,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Oct.csv",row.names=FALSE)
write.csv(Index_2016_Nov,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Nov.csv",row.names=FALSE)
write.csv(Index_2016_Dec,"/srv/shiny-server/DataAnalyticsPortal/data/PUB/Index_2016_Dec.csv",row.names=FALSE)