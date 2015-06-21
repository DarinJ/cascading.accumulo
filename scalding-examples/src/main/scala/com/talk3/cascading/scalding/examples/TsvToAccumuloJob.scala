package com.talk3.cascading.scalding.examples
import com.talk3.cascading.accumulo.AccumuloConnector.AccumuloConnectorBuilder
import com.talk3.cascading.accumulo.AccumuloScheme
import com.talk3.cascading.accumulo.scalding.AccumuloSource
import com.twitter.scalding._
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.accumulo.core.data.Value
class TsvToAccumuloJob(args: Args) extends Job(args){
    
  var conn=new AccumuloConnectorBuilder(args("instance"),args("zookeepers"),args("table"))
            .addUserName(args("user"))
            .addPassword(args("pass"))
            .setOffline(true).build()
  val source=Tsv(args("input"),('rowId,'columnFamily,'columnQualifier,'value))
  val sink=AccumuloSource(conn,new AccumuloScheme())
  source.insert(('columnVisibility,'timeStamp),(new ColumnVisibility("public"),1))
    .map('value->'value){x:String => new Value(x.getBytes())}
    .write(sink)
  override def config: Map[AnyRef, AnyRef] = {
    // resolves "ClassNotFoundException cascading.*" exception on a cluster
    super.config ++ Map("cascading.app.appjar.class" -> classOf[AccumuloToTsvJob])
  }
}