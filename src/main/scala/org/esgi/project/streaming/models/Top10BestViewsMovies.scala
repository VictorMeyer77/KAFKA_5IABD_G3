package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Top10BestViewsMovies(
                                   id: Long,
                                   title: String,
                                   views: Long,
                                 ) {
  def increment = this.copy( views = this.views + 1)


}
object Top10BestViewsMovies {
  implicit val format: OFormat[Top10BestViewsMovies] = Json.format[Top10BestViewsMovies]
}


