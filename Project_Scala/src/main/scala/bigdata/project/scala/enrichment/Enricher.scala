package bigdata.project.scala.enrichment

import java.io.{BufferedWriter, FileWriter}
import bigdata.project.scala.schema.{Calendar, Route, Trip}
import scala.io.Source

object Enricher extends App {
  val outputFile = new BufferedWriter(new FileWriter("/home/bd-user/Downloads/project.csv"))

  val tripList: List[Trip] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/trips.txt").getLines().toList.tail.map(Trip(_))
  val routeList: List[Route] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/routes.txt").getLines().toList.tail.map(Route(_))
  val calendarList: List[Calendar] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/calendar.txt").getLines().toList.tail.map(Calendar(_))

  // GenericMapJoin for Trip & Route by route_id
  val tripRoute: List[JoinOutput] = new GenericMapJoin[Trip, Route]((i) => i.route_id.toString)((j) => j.route_id.toString).join(tripList, routeList)

  // GenericNestedLoop of Calendar & RouteTrip by service_id
  val enrichedTrip = new GenericNestedLoop[Calendar, JoinOutput]((i, j) => i.service_id == j.left.asInstanceOf[Trip].service_id).join(calendarList, tripRoute)

  val output = enrichedTrip
    .map(joinOutput => {
      val t = Trip.toCsv(joinOutput.right.getOrElse(" ").asInstanceOf[JoinOutput].left.asInstanceOf[Trip])
      val r = Route.toCsv(joinOutput.right.getOrElse(" ").asInstanceOf[JoinOutput].right.getOrElse(" ").asInstanceOf[Route])
      val c = Calendar.toCsv(joinOutput.left.asInstanceOf[Calendar])
      t + "," + r + "," + c
    })

  // Print Output to CSV
  for (i <- output) {
    outputFile.newLine()
    outputFile.write(i)
  }
  outputFile.close()
}