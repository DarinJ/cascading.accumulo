package com.talk3.cascading.scalding.examples
import com.talk3.cascading.accumulo.AccumuloConnector.AccumuloConnectorBuilder
import com.talk3.cascading.accumulo.AccumuloScheme
import com.talk3.cascading.accumulo.scalding.AccumuloSource
import com.twitter.scalding._
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.accumulo.core.data.{Key, Value}
class TsvToAccumuloJob(args: Args) extends Job(args){
    
  var conn=new AccumuloConnectorBuilder(args("instance"),args("zookeepers"),args("table"))
            .addUserName(args("user"))
            .addPassword(args("pass"))
            .setOffline(args("offline").toBoolean).build()
  val source=Tsv(args("input"),('rowId,'columnFamily,'columnQualifier,'value))
  val sink=AccumuloSource(conn,new AccumuloScheme())
  source.insert(('columnVisibility,'timeStamp),(new ColumnVisibility("public"),1))
    .map('value->'value){x:String => new Value(x.getBytes())}
    .map(('rowId,'columnFamily,'columnQualifier,'columnVisibility)->'key) {
      x:(String, String, String, ColumnVisibility) => new Key(x._1,x._2,x._3, x._4, 0)
    }.groupBy('key){
    _.sortBy('key,'value).reducers(2)
    }
    .write(sink)
  override def config: Map[AnyRef, AnyRef] = {
    // resolves "ClassNotFoundException cascading.*" exception on a cluster
    super.config ++ Map("cascading.app.appjar.class" -> classOf[AccumuloToTsvJob])
  }
}