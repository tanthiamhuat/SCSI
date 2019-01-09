rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,tidyr,RPostgreSQL,data.table,rvest,xml2,stringr,fst)

NationalAverageRoomType <- (read_html("https://www.spgroup.com.sg/what-we-do/billing") %>%
                                    html_nodes("table") %>% 
                                    html_table())[[4]]

NationalAverageRoomType <- NationalAverageRoomType[1:5,]
colnames(NationalAverageRoomType)[1] <- "room_type"
NationalAverageRoomType[1:5,1] <- c("HDB01","HDB02","HDB03","HDB04","HDB05")

## Extract the date in a new colum
NationalAverageRoomType <- NationalAverageRoomType %>% gather(key = MonthYear, value = Consumption, -room_type)

## Split the date in month and year
NationalAverageRoomType$Month <- str_trim(unlist(lapply(str_split(NationalAverageRoomType$MonthYear, pattern = "'"), `[[`, 1)))
NationalAverageRoomType$Year <- as.numeric(str_trim(unlist(lapply(str_split(NationalAverageRoomType$MonthYear, pattern = "'"), `[[`, 2)))) + 2000

## Convert your month into factor so it would be well ordered then
NationalAverageRoomType$Month <- factor(NationalAverageRoomType$Month, levels = month.abb)

## Spread the data according to the months
NationalAverageRoomType <- NationalAverageRoomType %>% select(Year,room_type,Month,Consumption) %>% 
  spread(key = Month, value = Consumption) # if you prefer to have 0 instead of NA add the option [fill = 0] in the spread function

NationalAverageRoomType <- NationalAverageRoomType %>% dplyr::mutate(Source="SPGroup")
NationalAverageRoomType <- NationalAverageRoomType[,c(ncol(NationalAverageRoomType),1:(ncol(NationalAverageRoomType)-1))]

write.csv(NationalAverageRoomType,file="/srv/shiny-server/DataAnalyticsPortal/data/NationalAverageRoomType.csv",row.names = FALSE)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

library(timeDate)
lastdaylastmonth <- as.Date(timeLastDayInMonth(today()-20))

family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))

servicepoint <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id")) 

Punggol_2017 <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_2017.fst")[,1:12]
Punggol_thisyear <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_thisyear.fst")
Punggol <- rbind(Punggol_2017,Punggol_thisyear)
#Punggol <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last12months.fst")

Punggol <- Punggol %>% dplyr::filter(room_type != 'NIL' & adjusted_consumption != 'NA' &
                                     room_type != 'HDBCD' & service_point_sn !="3100660792") 
# contain data on sub meter, excluding childcare, and AHL

MonthlyConsumption <- Punggol %>% dplyr::mutate(Y=year(date_consumption),M=month(date_consumption)) %>%
                      group_by(service_point_sn,block,Y,M,room_type) %>% 
                      dplyr::summarise(MonthlyConsumption = sum(adjusted_consumption)) %>%
                      dplyr::rename(Month=M,Year=Y)

MonthlyConsumption$HHsize <- family_servicepoint$num_house_member[match(MonthlyConsumption$service_point_sn,family_servicepoint$service_point_sn)]

monthly_occupancy <- as.data.frame(tbl(con,"monthly_occupancy")) %>% dplyr::mutate(Year=year(lastupdated),Month=month(lastupdated))

MonthlyConsumption_OccupiedDays <- inner_join(MonthlyConsumption,monthly_occupancy,by=c("service_point_sn","Year","Month")) %>%
                                   dplyr::mutate(MonthlyConsumption=MonthlyConsumption/(occupancy_rate/100)) %>%
                                   dplyr::filter(date(lastupdated) <= lastdaylastmonth & occupancy_days !=0)

MonthlyConsumptionPerRoomType <- MonthlyConsumption_OccupiedDays %>%
                                 dplyr::group_by(Year,Month,room_type) %>%
                                 dplyr::summarise(MonthlyConsumptionPerRoomType=round(mean(MonthlyConsumption,na.rm = TRUE)/1000,1))

MonthlyConsumptionPerRoomType$Month <- factor(month.abb[MonthlyConsumptionPerRoomType$Month],levels = month.abb)

MonthlyConsumptionPerRoomType_wide <- spread(MonthlyConsumptionPerRoomType,Month,MonthlyConsumptionPerRoomType)

MonthlyConsumptionPerRoomType_wide <- MonthlyConsumptionPerRoomType_wide %>% dplyr::mutate(Source="Punggol") %>% as.data.frame()

MonthlyConsumptionPerRoomType_wide <- MonthlyConsumptionPerRoomType_wide[,c(ncol(MonthlyConsumptionPerRoomType_wide),1:(ncol(MonthlyConsumptionPerRoomType_wide)-1))]

CombinedSPGroupPunggol <- rbind(NationalAverageRoomType,MonthlyConsumptionPerRoomType_wide)

thisyear <- year(today())

CombinedSPGroupPunggol_thisyear <- CombinedSPGroupPunggol %>% dplyr::filter(Year==thisyear) %>% dplyr::arrange(room_type)
CombinedSPGroupPunggol_lastyear <- CombinedSPGroupPunggol %>% dplyr::filter(Year==thisyear-1) %>% dplyr::arrange(room_type)

Updated_DateTime_NationalAverageRoomType <- paste("Last Updated on ",now(),"."," Next Update on ",now()+months(1),".",sep="")

save(CombinedSPGroupPunggol_thisyear,CombinedSPGroupPunggol_lastyear,Updated_DateTime_NationalAverageRoomType,
     file="/srv/shiny-server/DataAnalyticsPortal/data/NationalAverageRoomType.RData")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_NationalAverageRoomType successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)