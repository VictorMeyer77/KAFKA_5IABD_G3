package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Score(
                   title: String,
                   score: Double
                 )


object Score {
  implicit val format: OFormat[Score] = Json.format[Score]
}
