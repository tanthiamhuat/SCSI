source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/ToDisconnect.R')  
source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

# PG_B3 #06 406 1/5/2018 11:12:42 PM 8795, 3100250322, id_service_point=223
# PG_B3 #07 394 2/5/2018 12:45:01 AM 5416, 3100249966, id_service_point=225
# PG_B3 #03 402 2/5/2018 2:54:02 AM 3443, 3100250190, id_service_point=197
# PG_B3 #08 392 2/5/2018 9:54:23 PM 2981, 3100249910, id_service_point=232

# PG_B3 #06 406 11/5/2018 3:12:40 PM 8068, 3100250322, id_service_point=223
# PG_B3 #03 402 11/5/2018 2:53:46 PM 2202, 3100250190, id_service_point=197

# PG_B3 #14 392 18/5/2018 9:21:46 PM 15959, 3100249934, id_service_point=280
# PG_B3 #06 406 18/5/2018 11:12:52 PM 5750, 3100250322, id_service_point=223

# PG_B3 #04 394 22/5/2018 1:23:06 PM 14977, 3100249954,id_service_point=201
# PG_B3 #04 398 22/5/2018 11:10:19 AM 4543, 3100250074,id_service_point=203
# PG_B3 #06 406 22/5/2018 11:12:52 AM 2999, 3100250322, id_service_point=223
# PG_B3 #07 394 22/5/2018 8:45:03 AM 11604, 3100249966, id_service_point=225
# PG_B3 #04 392 22/5/2018 7:56:03 AM 11409, 3100249894, id_service_point=200
# PG_B3 #03 396 22/5/2018 13:25:18 10835, 3100250010, id_service_point=194
# PG_B3 #15-394 2018-05-22 10:39:05 6784, 3100249998, id_service_point=289
# PG_B3 #13-394 2018-05-22 11:13:17 1012, 3100249990, id_service_point=273

# PG_B3 #03-406 22/5/2018 08:01:21 2947, 3100250310, id_service_point=199
# PG_B3 #14-392 2018-05-22 09:21:51 2090, 3100249934, id_service_point=280
# PG_B3 #03-404 2018-05-22 09:09:27 1142, 3100250250, id_service_point=198

# PG_B5 #06-332 2018-05-25 20:01:13,4900, 3100159831, id_service_point=451

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='223' and interpolated_consumption='8795'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='225' and interpolated_consumption='5416'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='197' and interpolated_consumption='3443'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='232' and interpolated_consumption='2981'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='223' and interpolated_consumption='8068'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='197' and interpolated_consumption='2202'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='280' and interpolated_consumption='15959'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='223' and interpolated_consumption='5750'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='201' and interpolated_consumption='14977'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='203' and interpolated_consumption='4543'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='223' and interpolated_consumption='2999'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='225' and interpolated_consumption='11604'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='200' and interpolated_consumption='11409'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='194' and interpolated_consumption='10835'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='289' and interpolated_consumption='6784'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='273' and interpolated_consumption='1012'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='199' and interpolated_consumption='2947'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='280' and interpolated_consumption='2090'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='198' and interpolated_consumption='1142'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})

sql_update <- "update consumption set adjusted_consumption=0 where id_service_point='451' and interpolated_consumption='4900'"
sapply(sql_update, function(x){dbSendQuery(mydb, x)})