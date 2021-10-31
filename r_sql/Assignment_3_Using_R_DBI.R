library(DBI)
conn <- dbConnect(RSQLite::SQLite(),"ariline2.db")


ontime00 <- read.csv("2000.csv.bz2", header = TRUE)
ontime01 <- read.csv("2001.csv.bz2", header = TRUE)
ontime02 <- read.csv("2002.csv.bz2", header = TRUE)
ontime03 <- read.csv("2003.csv.bz2", header = TRUE)
ontime04 <- read.csv("2004.csv.bz2", header = TRUE)
ontime05 <- read.csv("2005.csv.bz2", header = TRUE)

??dbWriteTable
dbWriteTable(conn, "ontime", ontime00)
dbWriteTable(conn, "ontime", ontime01, append = TRUE)
dbWriteTable(conn, "ontime", ontime02, append = TRUE)
dbWriteTable(conn, "ontime", ontime03, append = TRUE)
dbWriteTable(conn, "ontime", ontime04, append = TRUE)
dbWriteTable(conn, "ontime", ontime05, append = TRUE)

dbListTables(conn)

planesdata_db <- read.csv("plane-data.csv", header = TRUE)
dbWriteTable(conn, "planesdata", planesdata_db)

airports_db <- read.csv("airports.csv", header = TRUE)
dbWriteTable(conn, "airports", airports_db)

carriers_db <- read.csv("carriers.csv", header = TRUE)
dbWriteTable(conn, "carriers", carriers_db)

q1 <- dbSendQuery(conn, 
                  "SELECT model, AVG(DepDelay) as Avg_DepDelay 
                  FROM ontime JOIN planesdata on ontime.TailNum = planesdata.tailnum
                  WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
                  GROUP BY model
                  ORDER BY Avg_DepDelay 
                  LIMIT 3")
dbFetch(q1)
# Answer is plane model 737-2Y5 with lowest departure delay of 7.022026

q2 <- dbSendQuery(conn, 
                  "SELECT city, ontime.Dest as Destination, COUNT(*) as inbound_flights
                  FROM ontime JOIN airports on ontime.Dest = airports.iata
                  WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 
                  GROUP BY ontime.Dest
                  ORDER BY inbound_flights DESC
                  LIMIT 3")
dbFetch(q2)
# Answer is Chicago with 1963034 inbound flights 

q3 <- dbSendQuery(conn, 
                  "SELECT ontime.UniqueCarrier as Carrier, Description, COUNT(*) as Cancelled_Flights
                  FROM ontime JOIN carriers on ontime.UniqueCarrier = carriers.Code
                  WHERE ontime.Cancelled = 1
                  GROUP BY ontime.UniqueCarrier
                  ORDER BY Cancelled_Flights DESC
                  LIMIT 3")
dbFetch(q3)
# Answer is Delta Airlines Inc with 107851 cancelled flights

q4a <- dbSendQuery(conn,
                  "SELECT Description, COUNT(*) as Cancelled_Flights
                  FROM ontime JOIN carriers on ontime.UniqueCarrier = carriers.Code
                  WHERE ontime.Cancelled = 1
                  GROUP By ontime.UniqueCarrier")
dbFetch(q4a)

q4b <- dbSendQuery(conn,
                   "SELECT Description, COUNT(*) as Total_Flights
                   FROM ontime JOIN Carriers on ontime.UniqueCarrier = carriers.code
                   GROUP BY ontime.UniqueCarrier")
dbFetch(q4b)

dbCreateTable(conn, "q4a", c(Description = "TEXT", CancelledFlights = "INT"))
dbAppendTable(conn, "q4a", q4a)

q4a <- dbAppendTable(conn, "q4a", )

q4b <- dbCreateTable(conn, "q4b", c(Description = "TEXT", TotalFlights = "INT"))
q4b

q4 <- dbCreateTable(conn, "q4", c(Description = "TEXT", CancelledFlights = "INT", TotalFlights = "INT"))
q4

# Idk how to do query 4 haiz 















