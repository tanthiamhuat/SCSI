rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(rmarkdown,knitr,magrittr,plotly,dygraphs,kableExtra)

options(warn=-1)  # suppress all warnings

load("/srv/shiny-server/DataAnalyticsPortal/data/DataQualityMonitoringInformation_TEST.RData")
load("/srv/shiny-server/DataAnalyticsPortal/data/BillableMeters_IndexChecks_last30days.RData")
missing_service_point_sn["id_service_point"] <- NULL

rmarkdown::render("/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DataAnalyticsPortal/DataQuality/DataQualityMonitoringReport_TEST.Rmd")

from1 ="/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DataAnalyticsPortal/DataQuality/DataQualityMonitoringReport_TEST.html"
to="/var/www/html/reports"
file.copy(from1, to, overwrite = TRUE, recursive = F, copy.mode = TRUE)

time_taken <- proc.time() - ptm
ans <- paste("DataQualityMonitoringReport_TEST successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)

