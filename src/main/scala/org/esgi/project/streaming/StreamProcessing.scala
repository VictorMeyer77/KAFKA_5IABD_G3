package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.api.models.TitleWithScore
import org.esgi.project.streaming.models.{Likes, MeanScorePerMovie, MovieStat, Top10BestOrWorstMovies, Top10BestViewsMovies, View}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = "web-events-stream-app-G3"
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"

  //val lastMinuteStoreName = "ViewsOfLastMinute"
  //val lastFiveMinutesStoreName = "ViewsOfLast5Minutes"
  //val allStoreName = "ViewsOfAllTime"
  val scoreWithMovieOutputTableName: String = "meanScorePerMovie"
  val viewsPerMovieOutputTableName: String = "viewsPerMovie"
  val viewsTopicOutputTableName: String = "viewsTopicAll"
  val viewsPerMovieIdOutputTableName: String = "viewsCountPerID"
  val viewsPerMovieIdAndTitleOutputTableName: String = "viewsCountPerIDAndTitle"
  val viewsFromBeginningOutputTableName: String = "viewsPerMovieAll"
  val viewsOfLastMinuteOutputTableName: String = "viewsPerMovieLastMinute"
  val viewsOfLast5MinutesOutputTableName: String = "viewsPerMovieLast5Minutes"


  val lastMinuteByCategoryStoreName = "ViewsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "ViewsOfLast5MinutesByCategory"
  val thirtySecondsByCategoryStoreName: String = "ViewsOfAllTimeByCategory"

  //val meanLatencyForURLStoreName = "MeanLatencyForURL"

  val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // topic sources
  val views: KStream[String, View] = builder.stream[String, View](viewsTopicName)
  val likes: KStream[String, Likes] = builder.stream[String, Likes](likesTopicName)

  /**
   * -------------------
   * movies info
   * -------------------
   */
  val moviesGroupedByCategory: KGroupedStream[String, View] = views
    .map((_, view) => (view._id+"|"+view.view_category, view))
    .groupByKey

  val moviesGroupedByIdAndName: KTable[String, Long] = views
    .map((_, view) => (view._id+"|"+view.title, view))
    .groupByKey
    .count()(Materialized.as(viewsPerMovieIdAndTitleOutputTableName))

  val viewsGroupedById: KTable[Int, Long] = views
    .map((_, view) => (view._id, view))
    .groupByKey
    .count()(Materialized.as(viewsPerMovieIdOutputTableName))




  val viewsFromBeginning: KTable[String, Long] = moviesGroupedByCategory
    .count()(Materialized.as(viewsFromBeginningOutputTableName))


  val viewsOfLastMinute: KTable[Windowed[String], Long] = moviesGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(viewsOfLastMinuteOutputTableName))



  val viewsOfLast5Minutes: KTable[Windowed[String], Long] = moviesGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(viewsOfLast5MinutesOutputTableName))


  /**
   * -------------------
   * table for movies views
   * -------------------
   */
  val viewsGroupedByTitle: KTable[String, Long] = views
    .map((_, view) => (view.title+" (id="+view._id.toString+")", view))  //.map((_, view) => (view.title+" (id="+view._id+")", view))
    .groupByKey
    .count()(Materialized.as(viewsPerMovieOutputTableName))

  /**
   * -------------------
   * table for best/worst movies
   * -------------------
   */

  val viewsWithLikes: KStream[String, TitleWithScore] = views
    .join(likes)({ (view, like) =>
      TitleWithScore(title = view.title, score = like.score)
    }, JoinWindows.of(Duration.ofSeconds(5)))


  val meanScorePerMovie: KTable[String, MeanScorePerMovie] = viewsWithLikes
    .map((_, viewWithScore) => (viewWithScore.title, viewWithScore))
    .groupByKey
    .aggregate(MeanScorePerMovie.empty) { (_, newMovieWithScore, accumulator) =>
      accumulator.increment(score = newMovieWithScore.score).computeMeanScore
    }(Materialized.as(scoreWithMovieOutputTableName))



  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run() {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}