##########################################################
######## SPECIAL CHALLENGES
######## can be run once per day (and only once) at 15:00
##########################################################

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

# list of all family id
family <- as.data.frame(tbl(con,"family") %>% 
                          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD"))))
current_date <- Sys.Date()
current_time <-  Sys.time()

#####
### Special challenges
#####
all_special_challenges <- as.data.frame(tbl(con,"special_challenges")) 
id.max <- nrow(all_special_challenges)
# 
# ####  
# ## S01 => WELS Challenge
# ####
# 
# # list of all S01 challenge already done
# S01_done <- all_special_challenges %>% filter(code == 'S01')
# 
# # list of all families on which S01 can be trigger
# family_to_do <- setdiff(family$id,S01_done$family_id)
# 
# if(length(family_to_do) > 0){
#   # get the id service point of these families
#   id_service_point_todo <- family$id_service_point[match(family_to_do,family$id)]
#   max.flow <- as.data.frame(tbl(con,"flow") %>% filter(id_service_point %in% id_service_point_todo))
#   max.flow$family_id <- family$id[match(max.flow$id_service_point,family$id_service_point)] 
#   WELS.assess <- function(df){
#     df <- df %>% filter(flow_date >= current_date - days(30))
#     limit <- quantile(df$max_5_flow,75/100)
#     if(limit>700){return(data.frame(family_id = df$family_id[1],limit = limit))}else{return(NULL)}
#   }
#   WELS.challenge <- ddply(.data = max.flow,.variables = .(family_id),.fun = WELS.assess)
#   
#   if(NROW(WELS.challenge) > 0){
#     Create_S01 <- data.frame(code = "'S01'",
#                              family_id = WELS.challenge$family_id,
#                              results = "'AVAILABLE'",
#                              challenge_start_date = paste0("'",current_date,"'"),
#                              challenge_end_date =  current_date,
#                              created_date = paste0("'",current_time,"'"),
#                              updated_date = paste0("'",current_time,"'")
#     )
#     lubridate::month(Create_S01$challenge_end_date) <- lubridate::month(Create_S01$challenge_end_date) + 1
#     Create_S01$challenge_end_date =  paste0("'",Create_S01$challenge_end_date,"'")
#     Create_S01$id <- id.max+(1:nrow(Create_S01))
#     id.max <- id.max + nrow(Create_S01)
#     ## INSERT challenges in table
#     
#     sql_insert<- paste0("INSERT INTO special_challenges (code,family_id,results,challenge_start_date,challenge_end_date,created_date,updated_date,id) VALUES (",apply(Create_S01,1,function(x){paste0(x,collapse = ',')}),");")
#     sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
#   }
#   
# }

####
## S02 => Fix the leak
## S04 => no new leak
####
service_point <- as.data.frame(tbl(con,"service_point") %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B"))
service_point <- service_point[,c('id','service_point_sn')]
family$service_point_sn <- service_point$service_point_sn[match(family$id_service_point,service_point$id)]

leaks <- as.data.frame(tbl(con,"leak_alarm")) 
leaks <- leaks %>% filter(meter_type == 'SUB')
leaks$family_id <- family$id[match(leaks$service_point_sn,family$service_point_sn)] ###
leaks$start_date <- ymd(leaks$start_date)
leaks$end_date <- ymd(leaks$end_date)

# list of all S02
S02 <- all_special_challenges %>% filter(code == 'S02')
already_assigned_S02 <- S02$special_challenge_options

S02_running <- S02 %>% filter(results=='AVAILABLE')
#leaks$full_id <- paste(leaks$family_id,leaks$start_date,sep="_")
#S02_running$full_id <- paste(S02_running$family_id,S02_running$special_challenge_options,sep="_")

### Results for S02 (closed leaks) + S04 challenge
running_leaks <- leaks %>% filter(id %in% S02_running$special_challenge_options)
if(sum(running_leaks$status=='Close')>0){
  closed_leaks <- running_leaks %>% filter(status == 'Close')
  S02_toclose <- S02_running %>% filter(special_challenge_options %in% closed_leaks$id)
  Update_S02 <- data.frame(code = 'S02',
                           id = S02_toclose$id,
                           family_id = S02_toclose$family_id,
                           results = 'SUCCESS',
                           updated_date = current_time)
  ### UPDATE results
  sql_update<- paste0("UPDATE special_challenges SET results = '",Update_S02$results,"', updated_date  = '",Update_S02$updated_date,"' WHERE id = ",Update_S02$id," AND family_id = ",Update_S02$family_id," AND code = '",Update_S02$code,"';")
  sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  
  Create_S04 <- data.frame(code = "'S04'",
                           family_id = S02_toclose$family_id,
                           results = "'AVAILABLE'",
                           challenge_start_date = paste0("'",current_date,"'"),
                           challenge_end_date = current_date,
                           created_date = paste0("'",current_time,"'"),
                           updated_date = paste0("'",current_time,"'"))
  Create_S04$id <- id.max+(1:nrow(Create_S04))
  id.max <- id.max + nrow(Create_S04)
  lubridate::month(Create_S04$challenge_end_date) <- lubridate::month(Create_S04$challenge_end_date) + 3
  Create_S04$challenge_end_date <- paste0("'",Create_S04$challenge_end_date,"'")
  Create_S04$status <- "'-'"
  ### INSERT new challenges
  sql_insert<- paste0("INSERT INTO special_challenges (code,family_id,results,challenge_start_date,challenge_end_date,created_date,updated_date,id,status) VALUES (",apply(Create_S04,1,function(x){paste0(x,collapse = ',')}),");")
  sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
}

### S02 : New leaks appearance
new_leaks <- leaks %>% filter(status=='Open' & !(id %in% already_assigned_S02)) 

# retrieve all leaks appeared 3 days ago and still running 
# S02="AVAILABLE" for new leaks only, not existing Open Leaks
new_leaks <- unique(new_leaks %>% select(id,family_id,status,start_date,service_point_sn))
if(NROW(new_leaks)>0){
  Create_S02 <- data.frame(code="'S02'",
                           family_id = new_leaks$family_id,
                           results = "'AVAILABLE'",
                           special_challenge_options = new_leaks$id,
                           challenge_start_date = paste0("'",current_date,"'"),
                           created_date = paste0("'",current_time,"'"),
                           updated_date = paste0("'",current_time,"'"))
  Create_S02$id <- id.max+(1:nrow(Create_S02)) 
  id.max <- id.max + nrow(Create_S02)
  Create_S02$status <- "'-'"
  ### INSERT 
  sql_insert<- paste0("INSERT INTO special_challenges (code,family_id,results,special_challenge_options,challenge_start_date,created_date,updated_date,id,status) VALUES (",apply(Create_S02,1,function(x){paste0(x,collapse = ',')}),");")
  sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
}

### S04 : No new leak alerts during 3 months
S04 <- all_special_challenges %>% filter(code == 'S04')

#### Option 1 : failed directly displayed
S04_running <- S04 %>% filter(results=='AVAILABLE')
failed_S04 <- intersect(S04_running$family_id,new_leaks$family_id)
## Case 1 : a new leak has occurred
if(length(failed_S04)>0){
  S04_failed <- S04_running %>% filter(family_id %in% new_leaks$family_id)
  Update_S04_Fail <- data.frame(code = S04_failed$code,
                                id = S04_failed$id,
                                family_id = S04_failed$family_id,
                                results = 'FAILED',
                                updated_date = current_time)
  sql_update<- paste0("UPDATE special_challenges SET results = '",Update_S04_Fail$results,"', updated_date  = '",Update_S04_Fail$updated_date,"' WHERE id = ",Update_S04_Fail$id," AND family_id = ",Update_S04_Fail$family_id," AND code = '",Update_S04_Fail$code,"';")
  sapply(sql_update, function(x){dbSendQuery(mydb, x)})
}

##
## Case 2 : no new leaks
S04_ended  <- S04_running %>% filter(challenge_end_date == current_date-1)
if(NROW(S04_ended)>0){
  last3months.leak <- leaks %>% filter(start_date > S04_ended$challenge_start_date[1])
  success.family <- setdiff(S04_ended$family_id,last3months.leak$family_id)
  if(length(success.family)>0){
    S04_success <- S04_ended %>% filter(family_id %in% success.family)
    Update_S04_Success <- data.frame(code = S04_success$code,
                                     id = S04_success$id,
                                     family_id = S04_success$family_id,
                                     results = 'SUCCESS',
                                     updated_date = current_time)
    sql_update<- paste0("UPDATE special_challenges SET results = '",Update_S04_Success$results,"', updated_date  = '",Update_S04_Success$updated_date,"' WHERE id = ",Update_S04_Success$id," AND family_id = ",Update_S04_Success$family_id," AND code = '",Update_S04_Success$code,"';")
    sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  }
}

####
## S03 => Overcomsumption
####
overc <- as.data.frame(tbl(con,"overconsumption_alarm")) 
overc$family_id <- family$id[match(overc$service_point_sn,family$service_point_sn)]
new_overc <- overc %>% filter(overconsumption_date == (current_date - 1))

S03 <- all_special_challenges %>% filter(code == 'S03')
S03_running <- S03 %>% filter(results == 'AVAILABLE')

## Results of the challenge
# FAILED
failed.family <- intersect(S03_running$family_id,new_overc$family_id)
if(exists("Create_S02")){  
  failed.family_S02 <- intersect(S03_running$family_id,Create_S02$family_id)
  failed.family <- union(failed.family,failed.family_S02)
}
if(exists("Create_S04")){  
  failed.family_S04 <- intersect(S03_running$family_id,Create_S04$family_id)
  failed.family <- union(failed.family,failed.family_S04)
}

if(length(failed.family)>0){
  S03_failed <- S03_running %>% filter(family_id %in% failed.family)
  Update_S03_Fail <- data.frame(code = S03_failed$code,
                                id = S03_failed$id,
                                family_id = S03_failed$family_id,
                                results = 'FAILED',
                                updated_date = current_time)
  sql_update<- paste0("UPDATE special_challenges SET results = '",Update_S03_Fail$results,"', updated_date  = '",Update_S03_Fail$updated_date,"' WHERE id = ",Update_S03_Fail$id," AND family_id = ",Update_S03_Fail$family_id," AND code = '",Update_S03_Fail$code,"';")
  sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  
}
# SUCCESS
S03_ended  <- S03_running %>% filter(challenge_end_date < current_date)
if(NROW(S03_ended)>0){
  last3months.overc <- overc %>% filter(overconsumption_date > S03_ended$challenge_start_date[1])
  success.family <- setdiff(S03_ended$family_id,last3months.overc$family_id)
  if (length(failed.family)>0){success.family <- setdiff(success.family,failed.family)}
  if(length(success.family)>0){
    S03_success <- S03_ended %>% filter(family_id %in% success.family)
    Update_S03_Success <- data.frame(code = S03_success$code,
                                     id = S03_success$id,
                                     family_id = S03_success$family_id,
                                     results = 'SUCCESS',
                                     updated_date = current_time)
    sql_update<- paste0("UPDATE special_challenges SET results = '",Update_S03_Success$results,"', updated_date  = '",Update_S03_Success$updated_date,"' WHERE id = ",Update_S03_Success$id," AND family_id = ",Update_S03_Success$family_id," AND code = '",Update_S03_Success$code,"';")
    sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  }
}

## Assign new challenges
already_S02 <- as.data.frame(tbl(con,"special_challenges") %>% filter(code == 'S02')%>% filter(results=='AVAILABLE')) 
already_S04 <- as.data.frame(tbl(con,"special_challenges") %>% filter(code == 'S04')%>% filter(results=='AVAILABLE')) 
new_overc <- new_overc %>% filter(!family_id %in% already_S02$family_id)
new_overc <- new_overc %>% filter(!family_id %in% already_S04$family_id)

## need to get the latest overconsumption_date, so that there is no duplicates for same family_id
new_overc <- new_overc %>%
  dplyr::group_by(family_id) %>%
  dplyr::filter(overconsumption_date==current_date-1)

if(NROW(new_overc)>0){
  Create_S03 <- data.frame(code = "'S03'",
                           family_id = new_overc$family_id,
                           results = "'AVAILABLE'",
                           challenge_start_date = paste0("'",current_date,"'"),
                           challenge_end_date = current_date,
                           created_date = paste0("'",current_time,"'"),
                           updated_date = paste0("'",current_time,"'"))
  Create_S03$id <- id.max+(1:nrow(Create_S03))
  id.max <- id.max + nrow(Create_S03)
  lubridate::month(Create_S03$challenge_end_date) <- lubridate::month(Create_S03$challenge_end_date) + 3
  Create_S03$challenge_end_date <- paste0("'",Create_S03$challenge_end_date,"'")
  Create_S03$status <- "'-'"
  ### INSERT new challenges
  sql_insert<- paste0("INSERT INTO special_challenges (code,family_id,results,challenge_start_date,challenge_end_date,created_date,updated_date,id,status) VALUES (",apply(Create_S03,1,function(x){paste0(x,collapse = ',')}),");")
  sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
}
dbDisconnect(mydb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_SpecialChallenges successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)

# S02 - Fix the Leak
# S03 - No to overconsumption for 3 months
# S04 - No to leak for 3 months

## S03 and S04 should not happen at the same time
## S04 (Say no to Leak) has higher priority over S03 (Say not to High Usage)
## 3004521276 has both S03 and S04, familyid= 543
## S03 should be removed on SpecialChallenge table id=65

# If customer has a leak and overconsumption, it will only trigger S02 and not trigger S03. 
# If customer has fixed their leak, it will only trigger S04 and not trigger S03. 

# leak status=Open, S02=AVAILABLE  (one time till fixed)
# leak status=Close, S02=SUCCESS (leak is fixed) --> trigger S04=AVAILABLE
# if leak status=Open again within 3 months, S02=AVAILABLE & S04=FAIL
# 
# send leak on duration = 3 days and 10 days