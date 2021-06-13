package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MovieInfo(
                      _id: String,
                      title: String,
                      view_count: Long,
                      stats: MovieStats
                    )



object MovieInfo {
  implicit val format: OFormat[MovieInfo] = Json.format[MovieInfo]
}
