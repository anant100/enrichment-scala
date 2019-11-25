# enrichment-scala
Enrichment of 3 tables of GTFS STM database by using Scala io with Generic MapJoin &amp; Generic NestedLoopJoin

# Data set
This time we use STM GTFS data set. In this project, we continue the practice of enrichment (the most common data transformation task to do). To download the dataset, visit http://www.stm.info/en/about/developers
You have already analyzed the structure of the data. Note the path you have saved the dataset for further use in the application.

# Description

Enrich route with trip based on route_id and then enrich the result with Calendar based on service_id. At the end, you should write the final result in a CSV file with a proper header.
Parse trips data and enrich it with routes and calendar information. Note that here we create a new object that has information of trip, routes and calendar. It’s possible to do both joins at the same time. But, for simplicity, do one at a time. Hence, you will have an intermediate class as well.
a.	Trip
b.	Calendar
c.	Route
d.	TripRoute (intermediate class)
e.	EnrichedTrip (final class)

 
Trip
 

Calendar
 
         	       




Router
         	       


For the trip/route join use Map Join and for the calendar use nested loop join.
