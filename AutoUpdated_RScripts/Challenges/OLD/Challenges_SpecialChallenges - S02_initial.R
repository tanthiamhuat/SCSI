########################################################
######## CHALLENGES
######## S02 initial challenges => RUN ONCE BEFORE LAUNCHING THE APP
########################################################

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists


if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

# setwd("/srv/shiny-server/DataAnalyticsPortal/source")

# Establish connection
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")

# list of all family id
family <- as.data.frame(tbl(con,"family"))
family <- family %>% filter(status =='ACTIVE')
current_date <- Sys.Date()
current_time <-  Sys.time()

#####
### Special challenges
#####
all_special_challenges <- as.data.frame(tbl(con,"special_challenges")) 
id.max <- nrow(all_special_challenges)


####
## S02 => Fix the leak
####
service_point <- as.data.frame(tbl(con,"service_point"))
service_point <- service_point[,c('id','service_point_sn')]
family$service_point_sn <- service_point$service_point_sn[match(family$id_service_point,service_point$id)]

leaks <- as.data.frame(tbl(con,"leak_alarm")) 
leaks <- leaks %>% filter(meter_type == 'SUB')
leaks$family_id <- family$id[match(leaks$service_point_sn,family$service_point_sn)] ###
leaks$start_date <- ymd(leaks$start_date)
leaks$end_date <- ymd(leaks$end_date)

# list of all S02
S02 <- all_special_challenges %>% filter(code == 'S02')
S02_running<- S02 %>% filter(results=='AVAILABLE')
leaks$full_id <- paste(leaks$family_id,leaks$start_date,sep="_")

### S02 : New leaks appearance (appeared yesterday)
new_leaks <- leaks %>% filter(status == 'Open') # retrieve all leaks appeared yesterday and still running 
new_leaks <- unique(new_leaks %>% select(family_id,status,start_date,service_point_sn))

if(NROW(new_leaks)>0){
  Create_S02 <- data.frame(code="'S02'",
                           family_id = new_leaks$family_id,
                           results = "'AVAILABLE'",
                           special_challenge_options = paste0("'",new_leaks$start_date,"'"),
                           challenge_start_date = paste0("'",current_date,"'"),
                           created_date = paste0("'",current_time,"'"),
                           updated_date = paste0("'",current_time,"'"))
  Create_S02$id <- id.max+(1:nrow(Create_S02))
  id.max <- id.max + nrow(Create_S02)
  ### INSERT 
  sql_insert<- paste0("INSERT INTO special_challenges (code,family_id,results,special_challenge_options,challenge_start_date,created_date,updated_date,id) VALUES (",apply(Create_S02,1,function(x){paste0(x,collapse = ',')}),");")
  sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
}

dbDisconnect(mydb)
