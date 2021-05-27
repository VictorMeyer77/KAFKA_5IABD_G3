package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MovieStats(
                   past: Stat,
                   last_minute: Stat,
                   last_five_minutes: Stat
                 )


object MovieStats {
  implicit val format: OFormat[MovieStats] = Json.format[MovieStats]
}
