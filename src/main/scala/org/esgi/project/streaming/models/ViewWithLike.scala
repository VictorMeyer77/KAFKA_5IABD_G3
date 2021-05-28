package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewWithLike(
                 title: String,
                 score: Double
               )


object ViewWithLike {
  implicit val format: OFormat[ViewWithLike] = Json.format[ViewWithLike]
}
