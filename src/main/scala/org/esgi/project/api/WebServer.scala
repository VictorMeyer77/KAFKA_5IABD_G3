package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StoreQueryParameters}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{MeanScorePerMovie, Top10BestOrWorstMovies, ViewWithLike}

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * -------------------
 * Part.3 of exercise: Interactive Queries
 * -------------------
 */
object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          period match {
            case "30s" =>
              // TODO:
              val kvStore30Seconds: ReadOnlyWindowStore[String, Long] =

              complete(
                // TODO:
                ???
              )
            case _ =>
              // unhandled period asked
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },
      // TODO: TOP 10 best movies
      path("stats"/"ten"/"best"/"score") {
        get {
          val kvStoreBestMovies: ReadOnlyKeyValueStore[String, MeanScorePerMovie] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.scoreWithMovieOutputTableName,
                QueryableStoreTypes.keyValueStore[String, MeanScorePerMovie]()
              )
            )
          // fetch all available keys
          val availableKeys: List[String] = kvStoreBestMovies
            .all()
            .asScala
            .map(_.key)
            .toList
          complete(
            availableKeys
              .map(storeKeyToMeanScoreForTitle(kvStoreBestMovies))
              .sortBy(_.score)(implicitly[Ordering[Double]].reverse)
              .take(10)
          )

        }
      },
      // TODO: TOP 10 worst movies
      path("stats"/"ten"/"worst"/"score") {
        get {
          val kvStoreBestMovies: ReadOnlyKeyValueStore[String, MeanScorePerMovie] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.scoreWithMovieOutputTableName,
                QueryableStoreTypes.keyValueStore[String, MeanScorePerMovie]()
              )
            )
          // fetch all available keys
          val availableKeys: List[String] = kvStoreBestMovies
            .all()
            .asScala
            .map(_.key)
            .toList
          complete(
            availableKeys
              .map(storeKeyToMeanScoreForTitle(kvStoreBestMovies))
              .sortBy(_.score)(implicitly[Ordering[Double]])
              .take(10)
          )

        }
      },
      // TODO: TOP 10 best views
      path("stats"/"ten"/"best"/"views") {
        get {
          val kvStoreBestMovies: ReadOnlyKeyValueStore[String, MeanScorePerMovie] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.scoreWithMovieOutputTableName,
                QueryableStoreTypes.keyValueStore[String, MeanScorePerMovie]()
              )
            )
          // fetch all available keys
          val availableKeys: List[String] = kvStoreBestMovies
            .all()
            .asScala
            .map(_.key)
            .toList
          complete(
            availableKeys
              .map((_) => (_, 1))
              .count()
              .sortBy()(implicitly[Ordering[Double]])
              .take(10)
          )

        }
      }
    )
  }
  def storeKeyToMeanScoreForTitle(store: ReadOnlyKeyValueStore[String, MeanScorePerMovie])(key: String): ViewWithLike = {
    val row: MeanScorePerMovie = store.get(key)
    ViewWithLike(title = key, score = row.meanScore)
  }

  def storeKeyToMeanViewsForTitle(store: ReadOnlyKeyValueStore[String, MeanScorePerMovie])(key: String): ViewWithLike = {
//    val row: MeanScorePerMovie = store.get(key)
//    ViewWithLike(title = key, score = row.meanScore)
    // TODO
  }
}
