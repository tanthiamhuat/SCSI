pacman::p_load(rvest)
NationalAveragePerPremiseType <- (read_html("https://www.spgroup.com.sg/what-we-do/billing") %>%
                                  html_nodes("table") %>% 
                                  html_table())[[4]]

write.csv(NationalAveragePerPremiseType[1:5,],file="/srv/shiny-server/DataAnalyticsPortal/data/NationalAveragePerPremiseType1.csv",row.names = FALSE)
