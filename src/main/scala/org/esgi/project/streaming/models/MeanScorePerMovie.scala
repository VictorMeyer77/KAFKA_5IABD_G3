package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScorePerMovie(
                             sum_scores: Double,
                             count: Long,
                             meanScore: Double
                           ) {
  def increment(score: Double) = this.copy(sum_scores = this.sum_scores + score, count = this.count + 1)

  def computeMeanScore = this.copy(
    meanScore = this.sum_scores / this.count
  )
}


object MeanScorePerMovie {
  implicit val format: OFormat[MeanScorePerMovie] = Json.format[MeanScorePerMovie]

  def empty: MeanScorePerMovie = MeanScorePerMovie(0, 0, 0)
}

