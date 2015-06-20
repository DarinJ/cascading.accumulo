package com.talk3.cascading.accumulo.scalding

/**
Accumulo bindings for scalding.  We hope to place this in the public domain.
 */

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.{SinkMode, Tap}
import cascading.tuple.{Fields, _}
import com.talk3.cascading.accumulo.{AccumuloScheme, AccumuloTap, AccumuloConnector => JAccumuloConnector}
import com.twitter.scalding._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => R, Value, Key}
import org.apache.accumulo.core.util.{Pair => P}
import org.apache.hadoop.mapred._

import scala.collection.JavaConverters._

/**
 * Few ways to set up! Probably best to use the companion object!
 * @param conn
 * @param accumuloScheme
 */
class AccumuloSource(conn: JAccumuloConnector, var accumuloScheme: AccumuloScheme) extends Source {

		//begin default constructor
		val hdfsScheme=accumuloScheme.asInstanceOf[Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_]]
		//end default constructor
        override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_,_,_] = {
      mode match {
        case hdfsMode@Hdfs(_, _) => readOrWrite match {
          case Read => println("read Mode") ; (new AccumuloTap(conn, accumuloScheme)).asInstanceOf[Tap[_,_,_]]
          case Write => println("write Mode") ; (new AccumuloTap(conn, accumuloScheme,SinkMode.UPDATE)).asInstanceOf[Tap[_,_,_]]

        }
        case _ => throw new Error("can't run in local mode")
      }
    }

      

}

/**
 * Class originally used to create AccumuloConnector Objects
 * Favor com.talk3.cascading.accumulo.AccumuloConnector.AccumuloConnectorBuilder
 */
@deprecated
object AccumuloConnector{
  /**
   * Default generally not useful assumes all accumulo defaults for local mode.
   * Utility?
   */
  def apply(): JAccumuloConnector ={
    new JAccumuloConnector("instance", "localhost:2181", "root", "secret" , "table", "PUBLIC", 3, true)
  }
  /**
   * Default for reading offline tables
   */
  def apply(instance:String,zookeepers: String, table:String,user:String,password:String): JAccumuloConnector ={
    new JAccumuloConnector(instance, zookeepers, table, user, password,"", 3, true)
  }
  /**
   * Default for reading tables
   */
  def apply(instance:String,zookeepers: String, table:String,user:String,password:String, offline:Boolean): JAccumuloConnector ={
    new JAccumuloConnector(instance, zookeepers, table, user, password,"", 3, offline)
  }
  /**
   * If you want to read at with authorizations less than those of the accumulo user you're running on an offline table
   */
  def apply(instance:String,zookeepers: String, table:String,user:String,password:String,auths:String): JAccumuloConnector ={
    new JAccumuloConnector(instance, zookeepers, table, user, password,auths, 3, true)
  }
  /**
   * If you want to read at with authorizations less than those of the accumulo user you're running as.
   *
   */
  def apply(instance:String,zookeepers: String, table:String,user:String,password:String,auths:String,offline:Boolean): JAccumuloConnector ={
    new JAccumuloConnector(instance, zookeepers, table, user, password,auths, 3, offline)
  }

}



object AccumuloSource {


  /**
   * Creates an accumumlo source given a connectString of format:
   * accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * and a preconfigured AccumuloScheme
   */
  def apply(conn: JAccumuloConnector, accumuloScheme: AccumuloScheme): AccumuloSource = {
    new AccumuloSource(conn, accumuloScheme)
  }

  /**
   * Creates an accumumlo source given a connectString of format:
   * accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * and a preconfigured AccumuloScheme
   */
  def apply(connectionString: String, accumloScheme: AccumuloScheme): AccumuloSource = {
    AccumuloSource(JAccumuloConnector.fromString(connectionString), accumloScheme)
  }

  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param ranges Sequence of Ranges
   * @param iteratorSettings Sequence of iteratorSettings
   * Creates a Source automatically adds the ranges and iterators to the Scheme
   */
  def apply(connectionString: String, ranges: Seq[R], iteratorSettings: Seq[IteratorSetting]):AccumuloSource={
    var accumuloScheme=new AccumuloScheme()
    if(ranges!=null) accumuloScheme.addRanges(ranges.asJava)
    if(iteratorSettings!=null) accumuloScheme.addIteratorSettings(iteratorSettings.asJava)
    new AccumuloSource(JAccumuloConnector.fromString(connectionString),accumuloScheme)
  }

  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param fullTableScan a sanity check you're doing a full table scan - that may be big are you sure?
   */
	def apply(connectionString:String, fullTableScan:Boolean): AccumuloSource ={
    if(!fullTableScan)
      throw new RuntimeException("fullTableScan=false but no range!")
		AccumuloSource(connectionString, Seq[R](), Seq[IteratorSetting]())
	}

/*
  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param fullTableScan a sanity check you're doing a full table scan - that may be big are you sure?
   * @param iteratorSettings add any iterators
   */

  def apply(connectionString:String, fullTableScan:Boolean, iteratorSettings:Seq[IteratorSetting]): AccumuloSource ={
    AccumuloSource(connectionString, Seq[R](), iteratorSettings, "")
  }
  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param ranges limit the scan to a list of ranges
   */
	def apply(connectionString:String,ranges: Seq[R]): AccumuloSource ={
    AccumuloSource(connectionString, ranges, Seq[IteratorSetting](), "")
	}

  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param ranges limit the scan to a list of ranges
   * @param iteratorSettings add any iterators
   */
  def apply(connectionString:String,ranges: Seq[R], iteratorSettings:Seq[IteratorSetting]): AccumuloSource ={
    AccumuloSource(connectionString, ranges, Seq[IteratorSetting](), "")
  }

  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param queryString limit the rows and colums
   */
	def apply(connectionString:String,queryString:String): AccumuloSource ={
		AccumuloSource(connectionString, Seq[R](), Seq[IteratorSetting](), queryString)
	}

  /**
   * @param connectionString Sting format:
   *   accumulo://table?instance=inst&user=user&password=pass&zookeepers=localhost:2181&write_threads=3&auths=PUBLIC&offline=true
   * @param queryString limit the rows and colums
   * @param iteratorSettings add any iterators
   */
  def apply(connectionString:String,queryString:String,iteratorSettings:Seq[IteratorSetting]): AccumuloSource ={
    AccumuloSource(connectionString, Seq(), iteratorSettings, queryString)
  }


  /**
   * @param conn AccumuloConnector - built via AccumuloConnectorBuilder
   * @param ranges limit the scan to a list of ranges
   */
	def apply(conn:JAccumuloConnector,ranges: Seq[R]): AccumuloSource ={
		AccumuloSource(evalConnector(conn), ranges)
	}
  /**
   * @param conn AccumuloConnector - built via AccumuloConnectorBuilder
   * @param ranges limit the scan to a list of ranges
   * @param iteratorSettings add any iterators
   */
  def apply(conn:JAccumuloConnector,ranges: Seq[R], iteratorSettings:Seq[IteratorSetting]): AccumuloSource ={
    AccumuloSource(evalConnector(conn), ranges,iteratorSettings)
  }

  /**
   * @param conn AccumuloConnector - built via AccumuloConnectorBuilder
   * @param fullTableScan a sanity check you're doing a full table scan - that may be big are you sure?
   * @param iteratorSettings add any iterators
   */
 def apply(conn:JAccumuloConnector, fullTableScan:Boolean,iteratorSettings:Seq[IteratorSetting]): AccumuloSource ={
		AccumuloSource(evalConnector(conn), fullTableScan,iteratorSettings)
	}
*/
  /**
   * @param conn AccumuloConnector - built via AccumuloConnectorBuilder
   * @param fullTableScan a sanity check you're doing a full table scan - that may be big are you sure?
   */

  def apply(conn:JAccumuloConnector, fullTableScan:Boolean): AccumuloSource ={
    AccumuloSource(evalConnector(conn), fullTableScan)
  }

  private def evalConnector(conn: JAccumuloConnector): String ={
		conn.toString
	}
}
