package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StoreQueryParameters}
import org.esgi.project.api.models.{TitleWithScore, TitleWithViews}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{MeanScorePerMovie, MovieInfo, MovieStat, MovieStats, Top10BestOrWorstMovies, View}

import java.time.Instant
import scala.jdk.CollectionConverters._


object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("movie" / Segment) { movie_id: String =>
        get {
          // TODO: load all the stored tables and create a MovieInfo object
          val kvStoreMoviesFromBeginning: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.viewsFromBeginningOutputTableName,
                QueryableStoreTypes.keyValueStore[String, Long]()
              )
            )

          val kvStoreMoviesByIdAndTitle: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.viewsPerMovieIdAndTitleOutputTableName,
                QueryableStoreTypes.keyValueStore[String, Long]()
              )
            )

          val kvStoreMoviesLastMinute = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.viewsOfLastMinuteOutputTableName,
                QueryableStoreTypes.windowStore[String, Long]()
              )
            )

          val kvStoreMoviesLast5Minutes = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.viewsOfLast5MinutesOutputTableName,
                QueryableStoreTypes.windowStore[String, Long]()
              )
            )

          val timeNow: Instant = Instant.now()
          val timeLastMin: Instant = Instant.now().minusSeconds(60)
          val timeLast5Min: Instant = Instant.now().minusSeconds(300)

          val movie_stats_var: MovieStats = MovieStats(
            past = MovieStat(
              start_only = kvStoreMoviesFromBeginning.get(movie_id+"|start_only"),
              half = kvStoreMoviesFromBeginning.get(movie_id+"|half"),
              full = kvStoreMoviesFromBeginning.get(movie_id+"|full")
            ),
            last_minute = MovieStat(
              start_only = kvStoreMoviesLastMinute.fetch(movie_id+"|start_only", timeLastMin, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
              half = kvStoreMoviesLastMinute.fetch(movie_id+"|half", timeLastMin, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
              full = kvStoreMoviesLastMinute.fetch(movie_id+"|full", timeLastMin, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
            ),
            last_five_minutes = MovieStat(
              start_only = kvStoreMoviesLast5Minutes.fetch(movie_id+"|start_only", timeLast5Min, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
              half = kvStoreMoviesLast5Minutes.fetch(movie_id+"|half", timeLast5Min, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
              full = kvStoreMoviesLast5Minutes.fetch(movie_id+"|full", timeLast5Min, timeNow).asScala.toList.lastOption.map(_.value).getOrElse(0),
            )
          )


          val movie_info_var: MovieInfo = MovieInfo(
            _id = movie_id,
            title = kvStoreMoviesByIdAndTitle.all()
              .asScala
              .map(movie => (movie.key.split('|')(0), movie.key.split('|')(1) ) )
              .find(_._1 == movie_id)
              .map(_._2)
              .getOrElse("NON DEFINED"),
            view_count = kvStoreMoviesFromBeginning.get(movie_id+"|full"),
            stats = movie_stats_var,
          )

          complete(
            movie_info_var
          )
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
          val kvStoreBestMovies: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters.fromNameAndType(
                StreamProcessing.viewsPerMovieOutputTableName,
                QueryableStoreTypes.keyValueStore[String, Long]()
              )
            )
          complete(
            kvStoreBestMovies
              .all()
              .asScala
              .map{keyvalue => TitleWithViews(title = keyvalue.key, views = keyvalue.value)}
              .toList
              .sortBy(_.views)(implicitly[Ordering[Long]].reverse)
              .take(10)
          )

        }
      }
    )
  }
  def storeKeyToMeanScoreForTitle(store: ReadOnlyKeyValueStore[String, MeanScorePerMovie])(key: String): TitleWithScore = {
    val row: MeanScorePerMovie = store.get(key)
    TitleWithScore(title = key, score = row.meanScore)
  }


  val viewsFromBeginningOutputTableName: String = "viewsPerMovieAll"
  val viewsOfLastMinuteOutputTableName: String = "viewsPerMovieLastMinute"
  val viewsOfLast5MinutesOutputTableName: String = "viewsPerMovieLast5Minutes"

}
