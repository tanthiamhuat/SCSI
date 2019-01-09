library(dbplyr)
library(dplyr)
con <- src_postgres(host = "52.77.188.178", user = "thiamhuat", password = "thiamhuat1234##", dbname="amrstaging")
mydb <- dbConnect(PostgreSQL(), dbname="amrstaging",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")

# dbListTables(mydb)
#save(con,mydb,file="/srv/shiny-server/DataAnalyticsPortal/data/DB_Connections.RData")

# con_amrcms <- src_postgres(host = "52.77.188.178", user = "thiamhuat", 
#                            password = "thiamhuat1234##", dbname="proddb",
#                            options="-c search_path=amr_cms")

proddb <- dbConnect(PostgreSQL(), dbname="proddb",host="52.77.188.178",port=5432,user="thiamhuat",password="thiamhuat1234##")
