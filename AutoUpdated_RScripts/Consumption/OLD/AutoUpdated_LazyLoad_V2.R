# https://www.r-bloggers.com/lazy-load-with-archivist/
# http://yetanothermathprogrammingconsultant.blogspot.sg/2016/02/r-lazy-load-db-files.html
ptm <- proc.time()
setwd("/srv/shiny-server/DataAnalyticsPortal/source")
library(devtools)
if (!require(archivist)){
  install_github("archivist", "pbiecek")
  require(archivist)
}
library(tools)

# convert .RData -> .rdb/.rdx
lazyLoad = local({load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData"); 
  environment()})
tools:::makeLazyLoadDB(lazyLoad, "/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2")

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_LazyLoad_V2 successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)
