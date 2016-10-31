package sparkwithes

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.io.Source

/**
  * Created by Ayan on 10/31/2016.
  */
object ESandScalaApp extends App
{
  val transportClient = new TransportClient();
  transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

  val jsons = Source.fromFile("src/main/resources/sample.json").getLines().toList
  for (i <- 1 to jsons.length) {
    indexJson(jsons(i - 1), i.toString)
  }

  def indexJson(json: String, id: String) =
  {
    transportClient.prepareIndex("esa", "activityStream", id).setSource(json).execute().actionGet()
  }
}
