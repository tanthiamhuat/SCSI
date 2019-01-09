rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,ISOweek)

ptm <- proc.time()

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

lpcd <- as.data.frame(tbl(proddb,"comparison_view"))
comparison <- function(df){
  median_lpcd <- median(df$my_average_lpcd, na.rm = TRUE)
  eff_neig <- df %>% filter(my_average_lpcd <= median_lpcd)
  return(data.frame(average_block_lpcd = round(mean(df$my_average_lpcd, na.rm = TRUE)),
                    efficient_block_lpcd = round(mean(eff_neig$my_average_lpcd, na.rm = TRUE))))
}
BlockLPCD <- ddply(.data = lpcd,.variables = .(block),.fun = comparison,.progress = 'text')

## in case any of BlockLPCD$average_block_lpcd coincidentally are equal, we are 1 litres to any one of them.
AverageBlockLPCD <- BlockLPCD$average_block_lpcd
if (any(duplicated(AverageBlockLPCD)) == TRUE)
{
  AverageBlockLPCD[which(duplicated(AverageBlockLPCD))] <- AverageBlockLPCD[which(duplicated(AverageBlockLPCD))]+1 
}

BlockLPCD$average_block_lpcd <- AverageBlockLPCD
  
date_created <- data.frame(rep(today(),nrow(BlockLPCD)))
colnames(date_created) <- "date_created"
BlockLPCD <- cbind(BlockLPCD,date_created)
BlockLPCD$site <- c(rep("Punggol",2),rep("Yuhua",7),rep("Punggol",3))

BlockLPCD$id <- as.integer(rownames(BlockLPCD))
BlockLPCD <- BlockLPCD %>% dplyr::select_("id","block","average_block_lpcd","efficient_block_lpcd","date_created","site")

#BlockLPCD <- BlockLPCD %>% dplyr::filter(block %in% c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5","YH_B2","YH_B3","YH_B5","YH_B7"))
#BlockLPCD$block <- c("103C","199C","266A","613C","624C","214","222","228","237")

## PG_B1, 103C 
## PG_B2, 199C 
## PG_B3, 266A 
## PG_B4, 613C
## PG_B5, 624C 
## YH_B1, 213
## YH_B2, 214
## YH_B3, 222
## YH_B4, 224
## YH_B5, 228
## YH_B6, 234
## YH_B7, 237

dbSendQuery(proddb, "delete from block_lpcd")
dbWriteTable(proddb, "block_lpcd", BlockLPCD, append=TRUE, row.names=F, overwrite=FALSE)
dbDisconnect(proddb)

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_BlockLPCD successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)
