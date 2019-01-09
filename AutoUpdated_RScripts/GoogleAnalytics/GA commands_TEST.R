### Authentify yourself with the client ID and the Secret
client.id  <- '1053905067173-64e0fu5mm33ddtlgjo79d5e517oj68df.apps.googleusercontent.com'
client.secret <- '8JcqwJZkx7bW4emsicQKszHF'

#' info@watergowhere.gov.sg
#' @MRproject
# Need to run on local RStudio

require(RGoogleAnalytics)

# Authorize the Google Analytics account
# This need not be executed in every session once the token object is created 
# and saved
token <- Auth(client.id,client.secret)

# Save the token object for future sessions
save(token,file="./token_file")

# In future sessions it can be loaded by running load("./token_file")

ValidateToken(token)

# Build a list of all the Query Parameters
query.list <- Init(start.date = "2013-11-28",
                   end.date = "2013-12-04",
                   dimensions = "ga:date,ga:pagePath,ga:hour,ga:medium",
                   metrics = "ga:sessions,ga:pageviews",
                   max.results = 10000,
                   sort = "-ga:date",
                   table.id = "ga:33093633")

# Create the Query Builder object so that the query parameters are validated
ga.query <- QueryBuilder(query.list)

# Extract the data and store it in a data-frame
ga.data <- GetReportData(ga.query, token, split_daywise = T, delay = 5)

#### 

require(pacman)
pacman::p_load(dplyr,googleAnalyticsR,RGoogleAnalytics)

### Authentify yourself with the client ID and the Secret
client.id  <- '1053905067173-64e0fu5mm33ddtlgjo79d5e517oj68df.apps.googleusercontent.com'
client.secret <- '8JcqwJZkx7bW4emsicQKszHF'
if(!'oauth_token' %in% list.files()){
  token <- Auth(client.id,client.secret)
  save(token,file = "oauth_token")
}else{
  load('oauth_token')  
}
ValidateToken(token)
ga_auth()

### 

# the following id are used to catch up the data from the right viewId 
my_id <- 150526752 # for the mobile app
my_id2 <- 146145432 # for the desktop app

# the following lines enable to catch up the data
# it was quite tricky do understand how it work
# I advise you to go to this page https://developers.google.com/analytics/devguides/reporting/core/dimsmets
# keep in mind that the dimension called "dimension1" corresponds to the customer id

df_mobile <- google_analytics_4(my_id, 
                                date_range = c("2017-06-09", as.character(today())),
                                metrics = c('sessionDuration'),
                                dimensions = c("dimension1",'pagePath',"date",'dateHourMinute','sessionCount','sessionDurationBucket'),
                                max = -1)%>% 
  filter (pagePath %in% c('/alerts','/challenges','/faq','/home','/leaderboard','/my-account','/my-activity','/point-history','/rewards')) %>%
  mutate(Source = 'mobile')

df_desktop <- google_analytics_4(my_id2, 
                                 date_range = c("2017-06-09", as.character(today())),
                                 metrics = c('sessionDuration'),
                                 dimensions = c("dimension1",'pagePath',"date",'dateHourMinute','sessionCount','sessionDurationBucket'),
                                 max = -1)%>% 
  filter (pagePath %in% c('/alerts','/challenges','/faq','/home','/leaderboard','/my-account','/my-activity','/point-history','/rewards')) %>%
  mutate(Source = 'desktop')
