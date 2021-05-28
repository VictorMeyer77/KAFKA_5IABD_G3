package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MovieStats(
                   past: MovieStat,
                   last_minute: MovieStat,
                   last_five_minutes: MovieStat
                 )


object MovieStats {
  implicit val format: OFormat[MovieStats] = Json.format[MovieStats]
}
