package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Views(
                   views: List[View]
                 )


object Views {
  implicit val format: OFormat[Views] = Json.format[Views]
}
