import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> ds = env.fromElements("1", "2", "3").map(new MapFunction<String, Integer>() {
			@Override
			public Integer map(String value) throws Exception {
				return null;
			}
		});
		ds.print();

	}
}
