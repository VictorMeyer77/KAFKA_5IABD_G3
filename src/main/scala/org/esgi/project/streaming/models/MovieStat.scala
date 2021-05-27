package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MovieStat(
                   start_only: Int,
                   half: Int,
                   full: Int
                 )


object MovieStat {
  implicit val format: OFormat[MovieStat] = Json.format[MovieStat]
}
