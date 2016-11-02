package sparkwithes

import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import collection.mutable.HashMap

/**
  * Created by Ayan on 10/31/2016.
  */
object ESandScalaApp extends App
{
  val transportClient = new TransportClient();
  transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
  val index ="a1"
  val typeStr ="b1"
  val id ="1011"

  //indexJson(index,typeStr,id)
  getRecord(index,typeStr,id)

  /*
  val jsons = Source.fromFile("src/main/resources/sample.json").getLines().toList
  for (i <- 1 to jsons.length)
  {
    indexJson(jsons(i - 1), i.toString)
  }
  */

  def indexJson(index: String,typeStr: String,id: String) =
  {
    val data: HashMap[String, Object] = HashMap("title"->"t1","content"-> "c1","postDate"-> "p1")
    transportClient.prepareIndex(index, typeStr, id).setSource(data,"").execute().actionGet()
  }

  def get(json: String, id: String) =
  {
    //GetResponse res = transportClient.prepareGet("esa", "activityStream", id).execute().actionGet()
     val response: GetResponse = transportClient.prepareGet("kodcucom", "article", "1").execute().actionGet()
  }

  def getRecord(index: String,typeStr: String ,id: String) =
  {
    val response: GetResponse = transportClient.prepareGet(index, typeStr, id).execute().actionGet()
    print("response :"+response.getSource())

  }
}
