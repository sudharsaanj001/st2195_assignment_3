library(DBI)
library(dplyr)
library(dbplyr)

conn <- dbConnect(RSQLite::SQLite(), "ariline2.db")
dbListTables(conn)

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

planesdata_db <- read.csv("plane-data.csv", header = TRUE)
dbWriteTable(conn, "planesdata", planesdata_db)

# Querying for question 1 using dplyr begins 

ontime_db <- tbl(conn, "ontime")
planesdata_db <- tbl(conn, "planesdata")

??inner_join
planesdata_db <- rename(planesdata_db, "TailNum" = "tailnum")
ontime_db <- select(ontime_db, -"Year")
# removed the column Year cause it was giving me "Error ambiguous column name"
q1 <- inner_join(ontime_db, planesdata_db, by = "TailNum") %>%
      filter(Cancelled == 0 & Diverted == 0) %>%
      group_by(model) %>% summarize(model, avg_DepDelay = mean(DepDelay, na.rm = TRUE)) %>% 
      summarize(model, min_DepDelay = min(avg_DepDelay, na.rm = TRUE)) %>%
      select(model)
print(q1)

# Redoing q1 but with DepDelay > 0 
q1 <- inner_join(ontime_db, planesdata_db, by = "TailNum") %>%
  filter(Cancelled == 0 & Diverted == 0) %>%
  filter(DepDelay > 0) %>%
  group_by(model) %>% summarize(model, avg_DepDelay = mean(DepDelay, na.rm = TRUE)) %>% 
  summarize(model, min_DepDelay = min(avg_DepDelay, na.rm = TRUE)) %>%
  select(model, min_DepDelay)
print(q1)
# Answer is "737-2Y5" with min_DepDelay of 7.02

# Quering for question 2 using dplyr begins!

airports_db <- read.csv("airports.csv", header = TRUE)
dbWriteTable(conn, "airports", airports_db)
airports_db <- tbl(conn, "airports")

airports_db <- rename(airports_db, "Dest" = "iata")

q2 <- inner_join(ontime_db, airports_db, by = "Dest") %>%  
      filter(Cancelled == 0 & Diverted == 0) %>% 
      group_by(Dest) %>%
      summarize(city, Dest, inbound_flights = n()) %>%
      summarize(city, Dest, max_inbound_flights = max(inbound_flights)) %>%
      select(city, max_inbound_flights)
print(q2)
# Answer is "Chicago" which highest number of inbound flights of"1963034". 

# Querying for question 3 using dplyr begins!

carriers_db <- read.csv("carriers.csv", header = TRUE)
dbWriteTable(conn, "carriers", carriers_db)
carriers_db <- tbl(conn, "carriers")

carriers_db <- rename(carriers_db, "UniqueCarrier" = "Code")

q3 <- inner_join(ontime_db, carriers_db, by = "UniqueCarrier") %>%
      filter(Cancelled == 1) %>%
      group_by(UniqueCarrier) %>%
      summarize(UniqueCarrier, Description, cancelled_flights = n()) %>%
      summarize(UniqueCarrier, Description, max_cancelled_flights = max(cancelled_flights)) %>%
      select(UniqueCarrier, Description, max_cancelled_flights)
print(q3)
head(q3)
# Answer is "Delta Air Lines Inc" with highest number of cancelled flights of "107851"


# Querying for question 4 using dplyr begins!

# Creating a table for total flights 
q4a <- inner_join(ontime_db, carriers_db, by = "UniqueCarrier") %>%
       group_by(UniqueCarrier) %>%
       summarize(Description, total_flights =n())
head(q4a)

q4b <- inner_join(ontime_db, carriers_db, by = "UniqueCarrier") %>%
       filter(Cancelled == 1) %>%
       group_by(UniqueCarrier) %>%
       summarize(cancelled_flights = n())
head(q4b)

q4 <- inner_join(q4a, q4b, by = "UniqueCarrier") %>%
      summarize(UniqueCarrier, Description, relative_ratio = (total_flights/cancelled_flights)) %>%
      summarize(UniqueCarrier, Description, max_relative_ratio = min(relative_ratio, na.rm = TRUE)) %>%
      select(UniqueCarrier, Description, max_relative_ratio)
print(q4)
# Answer is "American Eagle Airlines Inc" but I couldn't get relative ratio. 
# I used total/cancelled because cancelled/total kept giving me integers that were 0. So I came up with an observation that is
## when you divide say 2/3 = 0.67 vs 3/4 = 0.75, then you switch the num & denom which gives 3/2 = 1.5 & 4/3 = 1.33, notice that
### the largest numbers have become the smallest numbers. So now I just have to choose the smallest number. 

# TIP: Constantly use head(q wtv) to check how the table is looking like at any particular stage. 























