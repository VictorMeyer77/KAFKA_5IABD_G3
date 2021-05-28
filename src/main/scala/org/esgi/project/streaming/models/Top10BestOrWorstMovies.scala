package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Top10BestOrWorstMovies(
                              sum_scores: Double,
                              count: Long,
                              meanScore: Double
                            ) {
  def increment(score: Double) = this.copy(sum_scores = this.sum_scores + score, count = this.count + 1)

  def computeMeanScore = this.copy(
    meanScore = this.sum_scores / this.count
  )
}


object Top10BestOrWorstMovies {
  implicit val format: OFormat[Top10BestOrWorstMovies] = Json.format[Top10BestOrWorstMovies]

  def empty: Top10BestOrWorstMovies = Top10BestOrWorstMovies(0, 0, 0)
}

