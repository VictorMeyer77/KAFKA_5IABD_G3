package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class TitleWithScore(
                 title: String,
                 score: Double
               )


object TitleWithScore {
  implicit val format: OFormat[TitleWithScore] = Json.format[TitleWithScore]
}
