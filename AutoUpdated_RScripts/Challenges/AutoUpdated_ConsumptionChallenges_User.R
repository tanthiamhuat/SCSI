########################################################
######## CHALLENGES
######## to be run once per month 
######## (1st day of month at 12:05PM)
########################################################

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
family <- as.data.frame(tbl(con,"family")) %>% 
          dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))
current_date <- Sys.Date()
current_time <-  Sys.time()

#####
### Consumption challenges
#####

  # data with all challenges
  all_challenges <- as.data.frame(tbl(con,"consumption_challenges_user")) 
  
  # data with running challenges
  running.chall <- all_challenges %>% dplyr::filter(results=='AVAILABLE')
  
  # data with all the results (highest_day / highest_week / highest_period)
  result_highest <- as.data.frame(tbl(con,"consumption_challenges")) 
  
  # If families have moved out (=not in the result_highest data.frame) challenge set to fail
  if(length(setdiff(running.chall$family_id,result_highest$family_id))>0){
    moveout.families <- setdiff(running.chall$family_id,result_highest$family_id)
    chall.movedout <- running.chall %>% filter(family_id %in% moveout.families)
    df.movedout <- data.frame(id = chall.movedout$id,family_id = chall.movedout$family_id, code = chall.movedout$code, results = 'FAILED', updated_date = current_time)
    df.movedout$code <- paste0("'",df.movedout$code,"'")
    df.movedout$results <- paste0("'",df.movedout$results,"'")
    df.movedout$updated_date <- paste0("'",df.movedout$updated_date,"'")
    sql_update<- paste("UPDATE consumption_challenges_user SET results = ",df.movedout$results,", updated_date  = ",df.movedout$updated_date,"WHERE id = ",df.movedout$id," AND family_id = ",df.movedout$family_id," AND code = ",df.movedout$code,";")
    sapply(sql_update, function(x){dbSendQuery(mydb, x)})
    
    running.chall <- running.chall %>% filter(!family_id %in% moveout.families)
  }
  
  if(nrow(running.chall)>0){
    # PART 1 : get the result of last consumption challenge
    get_challenge_results <- function(df){
      results_family <- result_highest %>% filter(family_id==df$family_id)
      if(NROW(results_family)>1){
        results_family <- results_family %>% filter(date_created == max(results_family$date_created))
      }
      res.chall <- switch(substr(df$code,1,3),
                          'C01' = results_family$highest_day_result,
                          'C02' = results_family$week_trend_result,
                          'C03' = results_family$highest_period_result)
      df.return <- data.frame(id = df$id,family_id = df$family_id, code = df$code, results = ifelse(res.chall,'SUCCESS','FAILED'), updated_date = current_time)
      return(df.return)
    }
    
    results_challenge <- ddply(.data = running.chall,.variables = .(family_id),.fun = get_challenge_results)
    results_challenge$code <- paste0("'",results_challenge$code,"'")
    results_challenge$results <- paste0("'",results_challenge$results,"'")
    results_challenge$updated_date <- paste0("'",results_challenge$updated_date,"'")
    ## UPDATE results in table : to do
    sql_update<- paste("UPDATE consumption_challenges_user SET results = ",results_challenge$results,", updated_date  = ",results_challenge$updated_date,"WHERE id = ",results_challenge$id," AND family_id = ",results_challenge$family_id," AND code = ",results_challenge$code,";")
    sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  }
  
  # PART 2 : assign new challenges
  assign_new_challenges <- function(df){
    ident <- df$id
  #  id1 <<- ident
    highest_family <- result_highest %>% dplyr::filter(family_id==ident)
    if(NROW(highest_family)==1){ ### /!\ family with id = 9, 453 doesn't appear in [consumption_challenges] table
      current.month <- month(current_date)
      challenge.code <- NA
      if(current.month %% 3 == 2){ # February, May, August or November => challenges based on the day of the week (C01)
        daysOfweek <- c('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
        challenge.code <- paste0('C01',LETTERS[match(highest_family$highest_day,daysOfweek)])
      }
      if(current.month %% 3 == 0){ # March, June, September or December
        challenge.code <- switch(highest_family$highest_week,
                                 'Weekdays' = 'C02A',
                                 'Weekends'= 'C02B')
      }
      if(current.month %% 3 == 1){ #January,  April, July or October
        challenge.code <- switch(highest_family$highest_period,
                                 'Morning' = 'C03A',
                                 'Afternoon'= 'C03B',
                                 'Evening' = 'C03C',
                                 'Night' = 'C03D')
      }
      if (is.null(challenge.code) || is.na(challenge.code)) return(NULL)  ## if error
      df.return <- data.frame(code = challenge.code,
                              results = 'AVAILABLE',
                              challenge_start_date = floor_date(current_date,unit='month'),
                              challenge_end_date = ceiling_date(current_date,unit='month'),
                              created_date = current_time,
                              updated_date = current_time
      )
      
      return(df.return)
    }
  }
  new_challenge <- ddply(.data = family,.variables = .(id),.fun = assign_new_challenges)
  colnames(new_challenge)[which(colnames(new_challenge)=='id')] <- 'family_id'
  
  new_challenge$id <- nrow(all_challenges)+(1:nrow(new_challenge))
  new_challenge$code <- paste0("'",new_challenge$code,"'")
  new_challenge$results <- paste0("'",new_challenge$results,"'")
  new_challenge$challenge_start_date <- paste0("'",new_challenge$challenge_start_date,"'")
  new_challenge$challenge_end_date <- paste0("'",new_challenge$challenge_end_date,"'")
  new_challenge$created_date <- paste0("'",new_challenge$created_date,"'")
  new_challenge$updated_date <- paste0("'",new_challenge$updated_date,"'")
  new_challenge$status <- "'-'"
 
  ## INSERT challenges in table : to do
  sql_insert<- paste0("INSERT INTO consumption_challenges_user (family_id,code,results,challenge_start_date,challenge_end_date,created_date,updated_date,id,status) VALUES (",apply(new_challenge,1,function(x){paste0(x,collapse = ',')}),");")
  sapply(sql_insert, function(x){dbSendQuery(mydb, x)})
  sapply(sql_insert, function(x){dbSendQuery(proddb, x)})

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_ConsumptionChallenges_User successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)