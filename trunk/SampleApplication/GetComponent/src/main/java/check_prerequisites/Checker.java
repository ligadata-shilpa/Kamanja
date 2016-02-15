package check_prerequisites;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

public class Checker {

	static StringWriter x = new StringWriter();

	public ArrayList<ComponentInfo> CheckComponent(String component, String hostArray[])
			throws IOException, InterruptedException, KeeperException {

		ArrayList<ComponentInfo> list = new ArrayList<ComponentInfo>();

		for (int i = 0; i < hostArray.length; i++) {
			ComponentInfo bean = new ComponentInfo();
			switch (component.trim().toLowerCase()) {
			case "zookeeper":
				ZookeeperHelper zookeeper = new ZookeeperHelper();
				zookeeper.askZookeeper(hostArray[i]);
				bean.setVersion(zookeeper.getVersion());
				bean.setStatus(zookeeper.getStatus());
				bean.setErrorMessage(zookeeper.getErrorMessage());
				bean.setInvocationNode(hostArray[i]);
				bean.setComponentName(component);
				break;
			case "kafka":
				System.out.println(hostArray[i]);
				KafkaHelper kafka = new KafkaHelper();
				kafka.CheckKafkaVersion(hostArray[i]);
				bean.setComponentName(component);
				break;
			case "hbase":
				HBaseHelper hbase = new HBaseHelper();
				hbase.AskHBase(hostArray[i]);
				bean.setVersion(hbase.getVersion());
				bean.setStatus(hbase.getStatus());
				bean.setErrorMessage(hbase.getErrorMessage());
				bean.setInvocationNode(hostArray[i]);
				bean.setComponentName(component);
				break;
			case "java":
				JavaHelper java = new JavaHelper();
				java.AskJava();
				bean.setVersion(java.getVersion());
				bean.setErrorMessage(java.getErrorMessage());
				bean.setComponentName(component);
				break;
			case "scala":
				ScalaHelper scala = new ScalaHelper();
				scala.AskScala();
				bean.setVersion(scala.getVersion());
				bean.setErrorMessage(scala.getErrorMessage());
				bean.setComponentName(component);
				break;
			}
			list.add(bean);
		}

		return list;
	}

	public String[] hostArray(String hostList) {
		return hostList.split(",");
	}

	public String CheckComponent(String args) {
		try {
			JsonUtility json = new JsonUtility();
			Checker checker = new Checker();
			json.JsonParse(args);
			String component = json.GetComponent();
			String hostList = json.GetHostList();
			String hostArray[] = checker.hostArray(hostList);
			ArrayList<ComponentInfo> list = checker.CheckComponent(component, hostArray);
			return new ObjectMapper().writeValueAsString(list);
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(x));
		}
		return null;
	}

}
