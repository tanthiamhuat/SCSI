rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(ISOweek,lubridate)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

# list of all family id
family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))

HouseholdMembers <- read.csv('http://www.tablebuilder.singstat.gov.sg/publicfacing/api/csv/title/12305.csv',header=TRUE,as.is=TRUE)
HouseholdMembers <- HouseholdMembers[10:13,c(1,ncol(HouseholdMembers))] %>% as.data.frame()
colnames(HouseholdMembers)[1] <- "room_type"
colnames(HouseholdMembers)[2] <- "num_house_member"
HouseholdMembers[1,1] <- "HDB01"
HouseholdMembers[2,1] <- "HDB03"
HouseholdMembers[3,1] <- "HDB04"
HouseholdMembers[4,1] <- "HDB05"

HDB02 <- data_frame(room_type="HDB02",num_house_member=HouseholdMembers[1,2])
HouseholdMembers <- bind_rows(HouseholdMembers, HDB02)
HouseholdMembers$num_house_member <- as.numeric(HouseholdMembers$num_house_member)

sql_update <- paste0("UPDATE family SET num_house_member = '",HouseholdMembers$num_house_member,"' WHERE room_type = '",HouseholdMembers$room_type,"'")
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_HouseholdMembers successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
