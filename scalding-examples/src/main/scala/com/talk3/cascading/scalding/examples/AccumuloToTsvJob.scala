package com.talk3.cascading.scalding.examples

import cascading.tuple.{TupleEntry, Fields}
import com.talk3.cascading.accumulo.AccumuloConnector.AccumuloConnectorBuilder
import com.talk3.cascading.accumulo.AccumuloScheme
import com.talk3.cascading.accumulo.scalding.AccumuloSource
import com.twitter.scalding._
import org.apache.accumulo.core.data.{Value, Key}
import collection.JavaConversions._
class AccumuloToTsvJob(args: Args) extends Job(args){
  @transient lazy val conn=new AccumuloConnectorBuilder(args("instance"),args("zookeepers"),args("table"))
            .addUserName(args("user"))
            .addPassword(args("pass"))
            .addAuths("public")
            .setOffline(args("offline").toBoolean).build()
  val source=AccumuloSource(conn,new AccumuloScheme())

  val sink=Tsv(args("input"),('rowId,'val))
  source.mapTo(('key,'value)->('rowId,'val)){
    x: (Key,Value) =>
    val (k,v) =x
    (k.getRow.toString,new String(v.get()))
  }.write(sink)
  
  override def config: Map[AnyRef, AnyRef] = {
    // resolves "ClassNotFoundException cascading.*" exception on a cluster
    super.config ++ Map("cascading.app.appjar.class" -> classOf[AccumuloToTsvJob])
  }
}