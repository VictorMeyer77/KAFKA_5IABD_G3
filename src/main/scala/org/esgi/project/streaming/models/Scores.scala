package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Scores(
                   scores: List[Score]
                 )


object Scores {
  implicit val format: OFormat[Scores] = Json.format[Scores]
}
