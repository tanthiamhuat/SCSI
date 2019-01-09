rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(dplyr,lubridate,RPostgreSQL,data.table,readxl,leaflet,tidyr)

con_amrcms <- src_postgres(host = "52.77.188.178", user = "thiamhuat", 
                           password = "thiamhuat1234##", dbname="proddb",
                           options="-c search_path=amr_cms")

usage_analysis <- as.data.frame(tbl(con_amrcms,"usage_analysis"))

con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="proddb")

consumption <- as.data.table(tbl(con,"consumption"))[date(date_consumption)>="2017-06-10"]

consumption_usage <- inner_join(consumption,usage_analysis,by=c("id"="consumption_id")) %>% 
                     dplyr::select_("id","family_id","activity_type_id","no_of_time","created_date","updated_at","is_in_3first","date_consumption.y")

colnames(consumption_usage)[which(names(consumption_usage) == "id")] <- "consumption_id"
colnames(consumption_usage)[which(names(consumption_usage) == "date_consumption.y")] <- "date_consumption"


