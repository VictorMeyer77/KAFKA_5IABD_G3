package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{MeanLatencyForURL, Likes, Views, VisitWithLatency}
import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = "web-events-stream-app-teacher"
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"

  //val lastMinuteStoreName = "ViewsOfLastMinute"
  //val lastFiveMinutesStoreName = "ViewsOfLast5Minutes"
  //val allStoreName = "ViewsOfAllTime"

  val lastMinuteByCategoryStoreName = "ViewsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "ViewsOfLast5MinutesByCategory"
  val thirtySecondsByCategoryStoreName: String = "ViewsOfAllTimeByCategory"

  //val meanLatencyForURLStoreName = "MeanLatencyForURL"

  val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // topic sources
  val views: KStream[String, Views] = builder.stream[String, Visit](viewsTopicName)
  val likes: KStream[String, Likes] = builder.stream[String, Metric](likesTopicName)

  /**
   * -------------------
   * Part.1 of exercise
   * -------------------
   */
  // Repartitioning views with visit URL as key, then group them by key
  val viewsGroupedByUrl: KGroupedStream[String, Visit] = views
    .map((_, visit) => (visit.url, visit))
    .groupByKey

  val viewsOfLast30Seconds: KTable[Windowed[String], Long] = viewsGroupedByUrl
    .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(1)))
    .count()

  val viewsOfLast1Minute: KTable[Windowed[String], Long] = viewsGroupedByUrl
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()

  val viewsOfLast5Minute: KTable[Windowed[String], Long] = viewsGroupedByUrl
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()

  /**
   * -------------------
   * Part.2 of exercise
   * -------------------
   */
  val viewsGroupedByCategory: KGroupedStream[String, Visit] = views
    .map((_, visit) => (visit.url.split("/")(1), visit))
    .groupByKey(Grouped.`with`)

  val viewsOfLast30SecondsByCategory: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(1)))
    .count()

  val viewsOfLast1MinuteByCategory: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()

  val viewsOfLast5MinuteByCategory: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()

  val viewsWithMetrics: KStream[String, VisitWithLatency] = views
    .join(likes)({ (visit, metric) =>
      VisitWithLatency(_id = visit._id, timestamp = visit.timestamp, sourceIp = visit.sourceIp, url = visit.url, latency = metric.latency)
    }, JoinWindows.of(Duration.ofSeconds(5)))

  val meanLatencyPerUrl: KTable[String, MeanLatencyForURL] = viewsWithMetrics
    .map((_, visitWithLatency) => (visitWithLatency.url, visitWithLatency))
    .groupByKey
    .aggregate(MeanLatencyForURL.empty) { (_, newVisitWithLatency, accumulator) =>
      accumulator.increment(latency = newVisitWithLatency.latency).computeMeanLatency
    }

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