package cromwell.backend.impl.vk

import java.math.BigInteger
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import cromwell.backend.BackendConfigurationDescriptor
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.http.client.methods.HttpPost
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory, TrustSelfSignedStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class VkConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val namespace = configurationDescriptor.backendConfig.getString("namespace")
  val storagePath = Option(configurationDescriptor.backendConfig.getString("storagePath"))
  val region = Option(configurationDescriptor.backendConfig.getString("region")).getOrElse("cn-north-4")
  val async = if(configurationDescriptor.backendConfig.hasPath("async")){configurationDescriptor.backendConfig.getString("async").toBoolean}else{false}
  val accessKey = configurationDescriptor.backendConfig.getString("accessKey")
  val secretKey = configurationDescriptor.backendConfig.getString("secretKey")
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
  var iamURL = s"https://iam.${region}.myhuaweicloud.com"
  var cciURL = s"https://cci.${region}.myhuaweicloud.com"

  if(region == "cn-north-7"){
    iamURL = "https://iam.cn-north-7.ulanqab.huawei.com"
    cciURL = "https://cci.cn-north-7.myhuaweicloud.com"
  }
  val token = Token(accessKey, secretKey, region, iamURL)

}

final case class Token(accessKey: String, secretKey:String, region: String, iamURL: String){

  var value = ""

  var time = -1L

  val overtime = 5*60*1000

  def getValue(): String = {
    if(value.isEmpty || isExpire(time)){
      try {
        value = initToken()
      } catch {
        case t: Throwable => println(t)
      }
      time = new Date().getTime
    }
    value
  }

  def isExpire(l: Long): Boolean = {
    if(time < 0L || new Date().getTime - l > overtime){
      true
    } else {
      false
    }
  }

  def initToken(): String = {
    retry()(Await.result[String](Future.fromTry(Try(getToken())), Duration.Inf))
  }

  def getToken(): String = {
    // get UTC time
    val timeZone = TimeZone.getTimeZone("GMT");
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val shortSimpleDateFormat = new SimpleDateFormat("HHmmss")
    simpleDateFormat.setTimeZone(timeZone)
    shortSimpleDateFormat.setTimeZone(timeZone)
    val date = new Date()
    val time = simpleDateFormat.format(date)
    val shortTime = shortSimpleDateFormat.format(date)
    val signTime = time + "T" + shortTime + "Z"

    val hwsDate = "X-Hws-Date"
    //    val region = ""
    val service = ""
    val terminalString = "hws_request"
    val authHeaderPrefix = "HWS-HMAC-SHA256"
    val hwsName = "HWS"
    val query = ""

    val projectName = region


    val signedHeaders = "x-hws-date"
    val canonicalHeadersOut = "x-hws-date:" + signTime + "\n"

    val requestBody = """{"auth":{"identity":{"methods":["hw_access_key"],"hw_access_key":{"access":{"key":"""" + accessKey + """"}}},"scope":{"project":{"name":"""" + projectName + """"}}}}"""
    val hexBody = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(requestBody.toString.getBytes("UTF-8"))))

    val canonicalRequestStr = "POST" + "\n" + "/v3/auth/tokens/" + "\n" + query + "\n" + canonicalHeadersOut + "\n" + signedHeaders + "\n" + hexBody
    val canonicalRequestStrToStr = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(canonicalRequestStr.toString.getBytes("UTF-8"))))

    val credentialString = time + "/" + region + "/" + service + "/" + terminalString
    val stringToSign =
      s"""${authHeaderPrefix}
         |${signTime}
         |${credentialString}
         |${canonicalRequestStrToStr}""".stripMargin

    // hmac256
    val secret = hwsName + secretKey
    val timeData = getHmacSH256(secret.getBytes("UTF-8"), time.getBytes("UTF-8"))
    val regionData = getHmacSH256(timeData, region.getBytes("UTF-8"))
    val serviceData = getHmacSH256(regionData, service.getBytes("UTF-8"))
    val credentials = getHmacSH256(serviceData, terminalString.getBytes("UTF-8"))
    val toSignature = getHmacSH256(credentials, stringToSign.getBytes("UTF-8"))
    val signature = toSignature.map("%02x" format _).mkString
    val signHeader = authHeaderPrefix + " Credential=" + accessKey + "/" + credentialString + ", " + "SignedHeaders=" +  signedHeaders + ", " + "Signature=" + signature

    // http post
    val builder = SSLContextBuilder.create().loadTrustMaterial(null,new TrustSelfSignedStrategy())
    var sslsf = new SSLConnectionSocketFactory(builder.build())
    if (region == "cn-north-7"){
      sslsf = new SSLConnectionSocketFactory(builder.build(), NoopHostnameVerifier.INSTANCE)
    }
    val httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build()
    val post = new HttpPost(iamURL + "/v3/auth/tokens")
    post.setHeader("X-Identity-Sign", signHeader)
    post.setHeader(hwsDate, signTime)
    post.setHeader("Content-Type", "application/json;charset=utf8")
    post.setEntity(new StringEntity(requestBody, "UTF-8"))
    val response = httpclient.execute(post)
    for (header <- response.getAllHeaders) {
      if (header.getName() == "X-Subject-Token") {
        return header.getValue()
      }
    }

    throw new Exception("using ak/sk to get token from IAM Failed.")
  }

  def getHmacSH256(key: Array[Byte], content: Array[Byte]): Array[Byte] = {
    val secret = new SecretKeySpec(key, "HmacSHA256")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(secret)
    mac.doFinal(content)
  }

  def retry[A](times: Int = 3, delay: Long = 1000)(any: => A, anyFail: => A = null): A = {
    Try(any) match {
      case Success(v) =>
        v
      case Failure(e) =>
        anyFail
        if (times > 0) {
          Thread.sleep(delay)
          retry(times - 1, delay)(any, anyFail)
        } else throw e
    }
  }

}
