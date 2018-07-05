package org.apache.flink.api.scala

import java.io.ByteArrayOutputStream
import java.util
import java.util.EnumSet

import org.apache.flink.api.java.MiniHadoopCluster
import org.apache.flink.api.scala.FlinkILoop
import org.apache.flink.api.scala.FlinkShell
import org.apache.hadoop.yarn.api.protocolrecords.{GetApplicationsRequest, GetApplicationsResponse}
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.junit.{Assert, Test}

import scala.Console
import scala.tools.nsc.interpreter.LoopCommands
import scala.tools.nsc.interpreter.Results
import org.junit.Assert.assertEquals


/**
  * Test for FlinkILoop under local mode.
  */
class FlinkShellTest {
  private val flinkShell = null

  @Test
  def testLocal(): Unit = {

    val out = new ByteArrayOutputStream()
    val thread = new Thread {
      override def run {
        Console.withOut(out) {
          FlinkShell.main(Array[String]("local"))
        }
      }
    }
    thread.start()
    // wait for flink-shell initialization completed
    Thread.sleep(60 * 1000)
    val flinkILoop = FlinkShell.iLoop
    val result = flinkILoop.interpreter.interpret("val dataSet = benv.fromElements(1,2,3)\ndataSet.count()")
    assertEquals(Results.Success, result)
    System.out.println(flinkILoop)
    System.out.println(out.toString)
    Assert.assertTrue(out.toString.endsWith("Long = 3"))
    thread.interrupt()
  }

  @Test
  def testYarnMode(): Unit = {
    val thread = new Thread() {
      val miniHadoopCluster = new MiniHadoopCluster(2)
      miniHadoopCluster.start()
    }
    thread.start()

//    Thread.sleep(1000*1000)
    FlinkShell.main(Array[String]("yarn", "-n", "1", //"-jm", "1024m", "-tm", "1024m",
      "-j", "/Users/jzhang/github/flink/build-target/lib/flink-dist_2.11-1.7-SNAPSHOT.jar"))

//    val out = new ByteArrayOutputStream()
//    val thread = new Thread {
//      override def run {
////        Console.withOut(out) {
//          FlinkShell.main(Array[String]("yarn", "-n", "1", "-jm", "1024", "-tm", "1024",
//            "-j", "/Users/jzhang/github/flink/build-target/lib/flink-dist_2.11-1.6-SNAPSHOT.jar"))
////        }
//      }
//    }
//    thread.start()
//    // wait for flink-shell initialization completed
//    Thread.sleep(120 * 1000)
//    val flinkILoop = FlinkShell.iLoop
//    val result = flinkILoop.interpreter.interpret("val dataSet = benv.fromElements(1,2,3)\ndataSet.count()")
//    assertEquals(Results.Success, result)
//    System.out.println(flinkILoop)
//    System.out.println(out.toString)
//    Assert.assertTrue(out.toString.endsWith("Long = 3"))

    // 1 yarn application launched
//    val request: GetApplicationsRequest = GetApplicationsRequest.newInstance(util.EnumSet.of(YarnApplicationState.RUNNING))
//    val response: GetApplicationsResponse = miniHadoopCluster.getYarnCluster.getResourceManager.getClientRMService.getApplications(request)
//    assertEquals(1, response.getApplicationList.size)


//    thread.interrupt()
//    Thread.sleep(60*1000)
//    miniHadoopCluster.stop()
  }
}

