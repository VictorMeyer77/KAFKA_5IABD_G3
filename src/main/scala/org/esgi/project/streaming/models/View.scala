package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class View(
                   title: String,
                   views: Int
                 )


object View {
  implicit val format: OFormat[View] = Json.format[View]
}
