package ca.mcit.kafka

case class Trip(
                 route_id: Int,
                 service_id: String,
                 trip_id: String,
                 trip_headsign: String,
                 direction_id: Int,
                 shape_id: Int,
                 wheelchair_accessible: Int,
                 note_fr: Option[String],
                 note_en: Option[String]
               )




case class Route(
                  route_id: Int,
                  agency_id: String,
                  route_short_name: String,
                  route_long_name: String,
                  route_type: String,
                  route_url: String,
                  route_color: String,
                  route_text_color: String
                )




case class Calendar(service_id:String,
                    monday:Int,
                    tuesday:Int,
                    wednesday:Int,
                    thursday:Int,
                    friday:Int,
                    saturday:Int,
                    sunday:Int,
                    start_date:Int,
                    end_date:Int
                   )



case class RouteTrip(trip: Trip, route: Route)

case class EnrichedTrip(trip: Trip, route:Option[Route], calendar: Option[Calendar])


object Trip extends App {
  def toCsv(trip: Trip): String = {
    trip.route_id + "," +
      trip.service_id + "," +
      trip.trip_id + "," +
      trip.trip_headsign + "," +
      trip.direction_id + "," +
      trip.shape_id + "," +
      trip.wheelchair_accessible + "," +
      trip.note_fr + "," +
      trip.note_en
  }
}

object Route extends App {
  def toCsv(route: Route): String = {
    route.route_id + "," +
      route.agency_id + "," +
      route.route_short_name + "," +
      route.route_long_name + "," +
      route.route_type + "," +
      route.route_url + "," +
      route.route_color + "," +
      route.route_text_color
  }
}

object Calendar extends App {
  def toCsv(calendar: Calendar): String = {
    calendar.service_id + "," +
      calendar.monday + "," +
      calendar.tuesday + "," +
      calendar.wednesday + "," +
      calendar.thursday + "," +
      calendar.friday + "," +
      calendar.saturday + "," +
      calendar.sunday + "," +
      calendar.start_date + "," +
      calendar.end_date
  }
}

object EnrichedTrip extends App{
  def toCsv(enriched:EnrichedTrip): String = {
    s"${Trip.toCsv(enriched.trip)}"
  }
}