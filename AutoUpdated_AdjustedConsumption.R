rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

# from consumption table, find status = C which has a huge value, and find the number of missing data status = M before that.
## average that huge compensataed value over the status=M and status=C
consumption_last6months_servicepoint <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last6months_Status.fst",as.data.table = TRUE)

consumption_2018_selected <- consumption_last6months_servicepoint %>%
                             dplyr::select_("id_service_point","interpolated_consumption","adjusted_consumption","date_consumption","status") %>%
                             dplyr::filter(status %in% c("M","C")) %>%
                             dplyr::arrange(id_service_point,date_consumption) 

data <- consumption_2018_selected %>% dplyr::group_by(id_service_point)

group = 1 
data$Group=1
for (i in 1:nrow(data)) {
  if (data[i,5]=="M") {
    data[i, 6] = group
  } else {
    data[i, 6] = group
    group = group + 1
  }
}

data2 <- data %>% dplyr::group_by(Group) %>%
         dplyr::summarise(adjusted_consumption=mean(interpolated_consumption)) %>%
         inner_join(data,by="Group")

sql_update <- paste0("UPDATE consumption SET adjusted_consumption = ",data2$adjusted_consumption.x," 
                      WHERE id_service_point = ",data2$id_service_point," AND
                      interpolated_consumption = ",data2$interpolated_consumption," AND 
                      date_consumption = '",data2$date_consumption,"';")
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_AdjustedConsumption successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log_DT.txt",append=TRUE)
