//package org.apache.flink.api.java;
//
//import org.apache.flink.api.scala.FlinkILoop;
//import org.apache.flink.api.scala.FlinkShell;
//
//import org.junit.Test;
//import scala.Console;
//import scala.tools.nsc.interpreter.LoopCommands;
//import scala.tools.nsc.interpreter.Results;
//
//import static org.junit.Assert.assertEquals;
//
///**
// * Test for FlinkILoop under local mode.
// */
//public class FlinkShellLocalTest {
//
//	private FlinkShell flinkShell;
//
//	@Test
//	public void testLocal() throws InterruptedException {
//		Thread thread = new Thread(() -> {
//			FlinkShell.main(new String[]{"local"});
//		});
//		thread.start();
//		Thread.sleep(60 * 1000);
//		FlinkILoop flinkILoop = FlinkShell.iLoop();
//		Console.setOut();
//		Results.Result result = flinkILoop.interpreter().interpret("val dataSet = benv.fromElements(1,2,3)\ndataSet.count()");
//		assertEquals(Results.Success$, result);
//		System.out.println(flinkILoop);
//		thread.interrupt();
//	}
//}
