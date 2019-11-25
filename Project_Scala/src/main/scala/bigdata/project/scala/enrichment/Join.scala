package bigdata.project.scala.enrichment

trait Join[L,R,Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Option[Any])

// GenericMapJoin
class GenericMapJoin[L,R] (val joinCond: (L) => String ) (val joinCond1: (R) => String ) extends Join[L,R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = {
    val l: Map[String, R] = b.map(b => joinCond1(b) -> b).toMap
    a.filter(a => l.contains(joinCond(a))).map(a => JoinOutput(a, Some(l(joinCond(a)))))
  }
}

// GenericNestedLoop
class GenericNestedLoop[L,R] (val joinCond: (L,R) => Boolean ) extends Join[L,R,JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    l <- a
    r <- b
    if joinCond(l, r)
  } yield JoinOutput(l, Some(r))
}

// Commented code -- Static MapJoin
/*class MapJoin extends Join[Trip, Route,TripRoute] {
  override def join(a: List[Trip], b: List[Route]): List[TripRoute] = {

    // Create a lookup Map on Route (b). Because Route_Id is common b/w both tables. (Always remember to Map on Small table)
    val t: Map[Int, Route] = b.map(route => route.route_id -> route).toMap

    a.filter(trip => t.contains(trip.route_id)).map(trip => TripRoute(trip, Some(t(trip.route_id))))
  }
}*/
