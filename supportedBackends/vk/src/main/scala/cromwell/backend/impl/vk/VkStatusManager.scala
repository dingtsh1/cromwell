package cromwell.backend.impl.vk

import java.util.Date
import java.util.concurrent.{ExecutionException, TimeUnit}

import akka.actor.{ActorContext, ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.google.gson.{Gson, JsonObject, JsonParser}
import cromwell.core.CromwellFatalExceptionMarker
import skuber.batch.Job
import cromwell.core.retry.Retry._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

final case class VkStatusManager(vkConfiguration: VkConfiguration) {

  val workflowMap = TrieMap[String, VkStatusItem]()

  val scheduledMap = TrieMap[String, Cancellable]()

  def submit(workflowId: String, context: ActorContext): Unit ={
    println(s"submit ${workflowId}")
    val item = VkStatusItem(workflowId, context, vkConfiguration)
    scheduledMap += (workflowId -> item.schedule())
    workflowMap += (workflowId -> item)
  }

  def remove(workflowId: String) ={
    val cancellable = scheduledMap.get(workflowId)
    if(!cancellable.isEmpty){
      println(s"remove ${workflowId}")
      if(cancellable.get.cancel()){
        workflowMap.remove(workflowId)
      }
    }
  }

  def getStatus(workflowId: String, jobId: String): Option[JsonObject] = {
    val item = workflowMap.get(workflowId)
    if(!item.isEmpty){
      item.get.getStatus(jobId)
    }else{
      None
    }
  }

  def setStatus(workflowId: String, job: Job) = {
    val item = workflowMap.get(workflowId)
    if(!item.isEmpty){
      item.get.setStatus(job)
    }
  }
}

final case class VkStatusItem(workflowId: String, context: ActorContext, vkConfiguration: VkConfiguration, initialDelay: FiniteDuration = FiniteDuration(0, TimeUnit.SECONDS), interval: FiniteDuration = FiniteDuration(10, TimeUnit.SECONDS)) {

  protected implicit def actorSystem: ActorSystem = context.system
  protected implicit lazy val ec: ExecutionContextExecutor = context.dispatcher
  protected implicit val materializer = ActorMaterializer()

  private val parser = new JsonParser()
  private val gson = new Gson()
  var statusMap = TrieMap[String, JsonObject]()
  val tempMap = TrieMap[String, JsonObject]()
  val scheduler = actorSystem.scheduler

  private val namespace = vkConfiguration.namespace
  //private val apiServerUrl = s"https://cci.${vkConfiguration.region}.myhuaweicloud.com"
  private val apiServerUrl = vkConfiguration.cciURL
  def getStatus(jobId: String): Option[JsonObject] = {
    val ret = statusMap.get(jobId)
    if(ret.isEmpty) {
      val tmp = tempMap.get(jobId)
      tmp
    }else{
      tempMap.remove(jobId)
      ret
    }
  }

  def setStatus(job: Job) = {
    println(s"set  ${job.name}")
    tempMap.put(job.name, gson.toJsonTree(job).getAsJsonObject)
  }

  def schedule()={
    scheduler.schedule(initialDelay, interval, new Runnable {
      override def run(): Unit = Await.result(fetch(workflowId), Duration.Inf)
    })
  }

  def fetch(workflowId: String)= {
    println(s"fetch ${workflowId} begin at ${new Date().toString}")
    makeRequest(HttpRequest(headers = List(RawHeader("X-Auth-Token", vkConfiguration.token.getValue())),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs?labelSelector=gcs-wdlexec-id%3D${workflowId}")) map {
      response => {
        println(s"fetch ${workflowId} end at ${new Date().toString}")
        val temp = TrieMap[String, JsonObject]()
        val jobList = response.get("items").getAsJsonArray
        val iterator = jobList.iterator()
        while(iterator.hasNext){
          val job = iterator.next().getAsJsonObject
          temp.put(job.get("metadata").getAsJsonObject.get("name").getAsString,job)
        }
//        for(job <- jobList){
//          temp.put(job.name, job)
//        }
        println(s"fetch size ${jobList.size}")
        statusMap = temp

      }
    }
  }

  private def makeRequest(request: HttpRequest): Future[JsonObject] = {
    withRetry(() => {
      for {
        response <- withRetry(() => Http().singleRequest(request))
        data <- if (response.status.isFailure()) {
          response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String) flatMap { errorBody =>
            Future.failed(new RuntimeException(s"Failed VK request: Code ${response.status.intValue()}, Body = $errorBody"))
          }
        } else {
          response.entity.withoutSizeLimit.toStrict(100000.millis).map[JsonObject] {
            res => {
              parser.parse(res.data.utf8String).getAsJsonObject
            }
          }
          //        Unmarshal(response.entity.withoutSizeLimit()).to[A]
          //        Await.result(response.entity.toStrict(10000.millis).map{
          //          res =>{
          //            Unmarshal(CloseDelimited(response.entity.contentType, res.dataBytes)).to[A]
          //          }
          //        },Duration.Inf)
        }
      } yield data
    },isTransient = isTransient)
  }

  def isTransient(throwable: Throwable): Boolean = {
    throwable match {
      case _: RateLimitException => true
      case _ => false
    }
  }

  def isFatal(throwable: Throwable): Boolean = throwable match {
    case _: RateLimitException => false
    case _: Error => true
    case _: RuntimeException => true
    case _: InterruptedException => true
    case _: CromwellFatalExceptionMarker => true
    case e: ExecutionException => Option(e.getCause).exists(isFatal)
    case _ => false
  }
}
