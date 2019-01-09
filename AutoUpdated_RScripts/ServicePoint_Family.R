rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(RPostgreSQL,plyr,dplyr,lubridate,data.table)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

servicepoint <- as.data.frame(tbl(con,"service_point"))
servicepoint_available <- servicepoint %>% dplyr::select_("id","site","block","service_point_sn") %>%
                          filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
servicepoint_available_Punggol <- servicepoint_available %>% filter(site=="Punggol")
servicepoint_available_Yuhua <- servicepoint_available %>% filter(site=="YUHUA")
servicepoint_available_Tuas <- servicepoint_available %>% filter(site=="Tuas")

servicepoint_blocks <- as.data.frame(tbl(con,"family")) %>% dplyr::filter(status=="ACTIVE") %>%
  dplyr::mutate(block=substr(address,1,5)) %>%
  dplyr::filter(!block %in% c("FAKE ","PUNGG")) %>%
  dplyr::group_by(block) %>%
  dplyr::summarise(BlockCount=n()) %>%  # need to add 3 to Punggol blocks for MAIN/BYPASS
  dplyr::mutate(BlockCount=ifelse(substr(block,1,2)=="PG",BlockCount+3,BlockCount))
