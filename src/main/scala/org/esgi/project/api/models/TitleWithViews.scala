package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class TitleWithViews(
                           title: String,
                           views: Long
                         )


object TitleWithViews {
  implicit val format: OFormat[TitleWithViews] = Json.format[TitleWithViews]
}
