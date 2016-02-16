package com.ligadata.kamanja.get_component;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import java.io.StringWriter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZookeeperHelper {
	// IZkConnection connection;
	int sessionTimout = 3000;
	ZooKeeper zConnection;
	String znode;
	String zdata;
	Stat stat;
	String errorMessage = null;
	String version;
	String nodeId;
	String status;
	String component;
	StringWriter errors = new StringWriter();
	StringUtility strutl = new StringUtility();

	private Watcher watcher = new Watcher() {
		@Override
		public void process(WatchedEvent arg0) {
			// TODO Auto-generated method stub

		}
	};

	public void ZConnect(String hostName) {
		try {
			zConnection = new ZooKeeper(hostName, sessionTimout, watcher);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
	}

	public void zCreate() {
		zdata = " 0";
		znode = "/LigaDataTest";
		try {
			if (zExist())
				zDelete();
			zConnection.create(znode, zdata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
	}

	public boolean zExist() {
		try {
			stat = zConnection.exists(znode, null);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
		if (stat != null)
			return true;
		return false;
	}

	public int zGet() {
		String result = null;
		int value = -1;
		byte[] bytes = null;
		try {
			bytes = zConnection.getData(znode, null, null);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
		result = new String(bytes);
		value = Integer.parseInt(result.trim());
		return value;
	}

	public void zSet() {
		zdata = " 10";
		try {
			zConnection.setData(znode, zdata.getBytes(), 1);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
	}

	public void zDelete() {
		try {
			zConnection.delete(znode, 0);
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
	}

	public void zClose() {
		try {
			zConnection.close();
		} catch (Exception e) {
			// e.printStackTrace(new PrintWriter(errors));
			// errorMessage += errors.toString();
			errorMessage += strutl.getStackTrace(e);
		}
	}

	public String zStatus() {
		if (!zConnection.getState().equals(States.CONNECTING) || zConnection == null) {
			// System.out.println(zConnection.getState());
			return "Fail";
		} else {
			// System.out.println(zConnection.getState());
			return "Success";
		}
	}

	// public String zGetVersion() {
	// StringBuffer output = new StringBuffer();
	// String command = "zkCli.sh start";
	// StringUtility str = new StringUtility();
	// output = str.ExecuteSHCommandInputStream(command);
	// return output.toString();
	// }
	public String CheckVersion() {
		FileUtility file = new FileUtility();
		file.ReadLogFile();
		String content = file.getFileContent();
		StringUtility str = new StringUtility();
		String doc = str.replaceSpacesFromString(content.trim().toLowerCase());
		int beginIndex = str.IndexOfString(doc, "zookeeper.version=");
		int lastIndex = str.IndexOfStringFrom(doc, beginIndex,",");
//		System.out.println(str.getWordBetweenIndex(doc, beginIndex + 18, lastIndex));
//		System.out.println(beginIndex);
//		System.out.println(lastIndex);
		//System.out.println(content);
		return str.getWordBetweenIndex(doc, beginIndex + 18, lastIndex);
	}

	public void askZookeeper(String host) throws InterruptedException, KeeperException {
		//// ZookeeperHelper zookeeper = new ZookeeperHelper();
		// String output = zookeeper.zGetVersion();
		// StringUtility str = new StringUtility();
		// String doc =
		// str.replaceSpacesFromString(output.toString().trim().toLowerCase());
		// int beginIndex = str.IndexOfString(doc, "version=");
		// int lastIndex = str.IndexOfString(doc, ",built");
		// String version = str.getWordBetweenIndex(doc, beginIndex + 8,
		// lastIndex);
		// System.out.println(version);
		// //zookeeper.ZConnect(host);
		// //zookeeper.CheckVersion();
		// //status = zookeeper.zStatus();
		ZConnect(host);
		version = CheckVersion();
		status = zStatus();
		// zookeeper.zCreate();
		// zookeeper.zSet();
		// zookeeper.zGet();
		// zookeeper.zClose();
		//System.out.println(errorMessage);
	}

	public int getSessionTimout() {
		return sessionTimout;
	}

	public void setSessionTimout(int sessionTimout) {
		this.sessionTimout = sessionTimout;
	}

	public ZooKeeper getzConnection() {
		return zConnection;
	}

	public void setzConnection(ZooKeeper zConnection) {
		this.zConnection = zConnection;
	}

	public String getZnode() {
		return znode;
	}

	public void setZnode(String znode) {
		this.znode = znode;
	}

	public String getZdata() {
		return zdata;
	}

	public void setZdata(String zdata) {
		this.zdata = zdata;
	}

	public Stat getStat() {
		return stat;
	}

	public void setStat(Stat stat) {
		this.stat = stat;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public StringWriter getErrors() {
		return errors;
	}

	public void setErrors(StringWriter errors) {
		this.errors = errors;
	}

	public Watcher getWatcher() {
		return watcher;
	}

	public void setWatcher(Watcher watcher) {
		this.watcher = watcher;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}
}
