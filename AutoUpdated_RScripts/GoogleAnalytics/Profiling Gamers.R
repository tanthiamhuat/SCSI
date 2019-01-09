rm(list=ls())  # remove all variables
memory.limit(1e+13)
cat("\014")    # clear Console

if (dev.cur()!=1) {dev.off()} # clear R plots if exists
# setwd('D:/Karim/Singapour/WP3/KPI')

require(pacman)
pacman::p_load(RPostgreSQL,plyr,dplyr,data.table,lubridate,stringr,readxl,xlsx,tidyr,ggplot2,googleAnalyticsR,RGoogleAnalytics)

# connections to databases
# amr_staging public
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
# proddb public
con_proddb <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="proddb")
# proddb amr_cms
con_amrcms <- src_postgres(host = "52.77.188.178", user = "thiamhuat", 
                           password = "thiamhuat1234##", dbname="proddb",
                           options="-c search_path=amr_cms")

Family <- as.data.frame(tbl(con_proddb,"family"))
ServicePoint <- as.data.frame(tbl(con_proddb,"service_point"))
customer <- as.data.frame(tbl(con_amrcms,"customer"))
customer$id <- as.character(customer$id)

customer_points <- as.data.frame(tbl(con_amrcms,"customer_points"))
customer_points <- customer_points %>% dplyr::filter(!customer_id %in% c(464,552))
customer_points$family_id <- customer$id_family[match(customer_points$customer_id,customer$id)]

customer_points$Type.reward <- 'Others'
customer_points$Type.reward[which(customer_points$reward_category == 'daily_login')] <- 'Login'
chall.rewards <- c('bonus_challenges','identify_usage','facebook_discussion','manual_bonus50','manual_snapit','bonus_reward','bonus_reward_100')
customer_points$Type.reward[which(customer_points$reward_category %in% chall.rewards)] <- 'Bonus Challenge'
customer_points$Type.reward[which(customer_points$reward_category == 'total_savings')] <- 'Water Savings'
ws_chall <- c('consumption_challenges','manual_consumptionchallenge','special_challenges','manual_leakfix')
customer_points$Type.reward[which(customer_points$reward_category %in% ws_chall)] <- 'WS Challenge'

customer_points$is_main <- customer$is_main[match(customer_points$customer_id,customer$id)]

name_this_chall.not_main <- customer_points %>% dplyr::filter(reward_category == 'identify_usage' & !(is_main))

Points <- anti_join(customer_points,name_this_chall.not_main)

Login <- Points %>% dplyr::group_by(family_id,Type.reward) %>% tally() %>% spread(Type.reward,n,fill = 0) %>%
  dplyr::filter(Login >= 5)

Profile.online <- Points %>% dplyr::group_by(customer_id,Type.reward) %>% dplyr::summarise(Win=n()) %>%
  spread(key = Type.reward,value = Win,fill = 0)

#######

WS <- Profile.online %>% dplyr::filter(Login > 1) %>% dplyr::filter(`Water Savings`> 0) %>% 
  dplyr::filter(`Bonus Challenge`> 0)
WS$profile_type <- 'Water Savers'

CWS <- Profile.online %>% dplyr::filter(Login > 1) %>% dplyr::filter(`Water Savings`> 0) %>% 
  dplyr::filter(`Bonus Challenge`== 0)
CWS$profile_type <- 'Curious Water Savers'

OTWS <- Profile.online %>% filter(Login == 1) %>% filter(`Water Savings` > 0)
OTWS$profile_type <- 'One Time Login Water Savers'

Gamers <- Profile.online %>% dplyr::filter(Login > 1) %>% dplyr::filter(`Water Savings`== 0) %>% 
  dplyr::filter(`Bonus Challenge`> 0)
Gamers$profile_type <- 'Gamers'

OTGam <- Profile.online %>% dplyr::filter(Login == 1) %>% dplyr::filter(`Water Savings` == 0) 
OTGam$profile_type <- 'One Time Login Gamers'

NoLog <- Profile.online %>% dplyr::filter(Login == 0)
NoLog$profile_type <- 'Never Login'

ProfileOnline <- rbind(WS,CWS)
ProfileOnline <- rbind(ProfileOnline,OTWS)
ProfileOnline <- rbind(ProfileOnline,Gamers)
ProfileOnline <- rbind(ProfileOnline,OTGam)
ProfileOnline <- rbind(ProfileOnline,NoLog)
ProfileOnline$customer_id <- as.character(ProfileOnline$customer_id)

Prof <- inner_join(customer[,-which(names(customer)=='profile_type')],ProfileOnline[,c('customer_id','profile_type')],by=c('id'='customer_id'))
Type.players <- Prof %>% group_by(id_family,profile_type) %>% tally() %>% 
  spread(profile_type,n,fill=0)
save.prof <- Prof %>% select(id,id_family,username,age_range,profile_type,is_main)

###

X <- read_excel('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/GoogleAnalytics/All LPCD.xlsx',sheet = 'Online') 
X$service_point_sn <- as.character(X$service_point_sn)

Family$service_point_sn <- ServicePoint$service_point_sn[match(Family$id_service_point,ServicePoint$id)]

DB <- Family %>% filter(id %in% save.prof$id_family) %>% 
  select(id,service_point_sn,num_house_member,room_type)

DB <- inner_join(DB,X) 
FullTable <- inner_join(save.prof,DB,by = c('id_family'='id'))

########################################
########### Google Analytics ###########
########################################
Online.cust <- customer$id[customer$is_active]

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
#Use my_accounts to find the viewId. Make sure to replace this with your viewId.
my_id <- 150526752
my_id2 <- 146145432

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


Session <- rbind(df_mobile,df_desktop)
Session$duration <- as.numeric(Session$sessionDurationBucket)
cust <- unique(Session$dimension1)
cat(setdiff(cust,Online.cust),'\n')

User_session <- Session %>% select(dimension1,date,sessionCount,duration) %>% distinct()
User_session <- inner_join(User_session,select(customer,id,username), by = c('dimension1' = 'id'))

Nbsession <- User_session %>% filter(duration > 1) %>% group_by(dimension1,username,date) %>% tally() 

## Nb Login
Nbsession <- User_session %>% filter(duration > 1) %>% select(dimension1,username,date) %>% distinct() %>% 
  group_by(dimension1,username) %>% tally()

res <- Nbsession

## Login more than once a day
MultiLogin <- User_session %>% filter(duration > 1) %>% group_by(dimension1,username,date) %>% tally() %>% 
  group_by(dimension1,username) %>% dplyr::summarise(n.avg = mean(n),n.med = median(n),nb_sup1=sum(n>1),p_sup1 =sum(n>1)/n())
MultiLogin$LoginMostMoreThanOnce <- (MultiLogin$n.med>1)
MultiLogin$LoginFewMoreThanOnce <- (MultiLogin$n.avg>1)
res <- inner_join(res,select(MultiLogin,dimension1,LoginMostMoreThanOnce,LoginFewMoreThanOnce))

# Most viewed page (excluding home)
Page_View <- Session %>% 
  filter(duration > 1) %>% 
  group_by(dimension1,pagePath) %>% tally() %>% mutate(nb = ifelse(n==1,0,n)) %>% 
  select(dimension1,pagePath,nb) %>% spread(pagePath,nb,fill = 0)

tmp.page <- Page_View
tmp.page[,c('dimension1','/home')] <- NULL
names(tmp.page) <- str_sub(names(tmp.page),start=2)

Page_View$most_viewed <- apply(tmp.page,1,function(x){names(tmp.page)[which.max(x)]})
res <- inner_join(res,select(Page_View,dimension1,most_viewed))


## Most engaged time
# Morning: 6:01am-12pm
# Afternoon: 12:01pm-6pm
# Evening: 6:01pm - 12am
# Night: 12:01am - 6am
period_day <- function(datetime){
  m <- minute(datetime)
  h <- hour(datetime)
  if(h%%6==0){
    h <- h-1
    if (h<0) h <-23 
  }
  ifelse(h<6,'Night',ifelse(h<12,'Morning',ifelse(h<18,'Afternoon','Evening')))
}
Eng.time <- Session %>% 
  filter(duration > 1) %>% 
  mutate(time = ymd_hm(dateHourMinute)) %>%
  group_by(dimension1,date,sessionCount) %>%
  dplyr::summarise(x = min(time)) %>% mutate(period = period_day(x))
Eng.period <- Eng.time %>% group_by(dimension1,period) %>% tally() %>% spread(period,n,fill = 0)
Eng.period$highest_per <- apply(Eng.period[,-1],1,function(x){names(Eng.period[,-1])[which.max(x)]})
res <- inner_join(res,select(Eng.period,dimension1,highest_per))


# Average session time (check which device is the most popular):
User_session_2 <- Session %>% select(dimension1,date,sessionCount,duration,Source) %>% distinct()
Session.time <- User_session_2 %>% filter(duration>1) %>% 
  group_by(dimension1,Source) %>% dplyr::summarise(x = mean(duration)) %>% 
  spread(Source,x,fill = 0)
names(Session.time)[-1] <- paste0('time.',names(Session.time)[-1])
res <- inner_join(res,Session.time)

res <- inner_join(select(customer,id,id_family),res,by = c('id'='dimension1'))

FullTable <- inner_join(FullTable,res)

wb = createWorkbook()
sheet = createSheet(wb, "Customer")
addDataFrame(as.data.frame(FullTable), sheet=sheet, startColumn=1, row.names=FALSE)
saveWorkbook(wb, "Online customer features.xlsx")

