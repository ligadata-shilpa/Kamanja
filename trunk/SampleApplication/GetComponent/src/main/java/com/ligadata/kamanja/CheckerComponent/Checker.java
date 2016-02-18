package com.ligadata.kamanja.CheckerComponent;

import java.io.*;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONArray;

public class Checker {

    static StringWriter x = new StringWriter();

    public /* ArrayList<ComponentInfo> */ComponentInfo CheckComponent(String component,
                                                                      String host/* String hostArray[] */) throws IOException, InterruptedException, KeeperException {

        // ArrayList<ComponentInfo> list = new ArrayList<ComponentInfo>();

        // for (int i = 0; i < hostArray.length; i++) {
        ComponentInfo bean = new ComponentInfo();
        switch (component.trim().toLowerCase()) {
            case "zookeeper":
                ZookeeperHelper zookeeper = new ZookeeperHelper();
                zookeeper.askZookeeper(/* hostArray[i] */host);
                bean.setVersion(zookeeper.getVersion());
                bean.setStatus(zookeeper.getStatus());
                bean.setErrorMessage(zookeeper.getErrorMessage());
                // bean.setInvocationNode(/* hostArray[i] */null);
                bean.setComponentName(component);
                break;
            case "kafka":
                // System.out.println(hostArray[i]);
                KafkaHelper kafka = new KafkaHelper();
                kafka.AskKafka(/* hostArray[i] */host);
                // kafka.CheckKafkaVersion(hostArray[i]);
                bean.setStatus(kafka.getStatus());
                bean.setErrorMessage(kafka.getErrorMessage());
                bean.setComponentName(component);
                bean.setVersion(null);
                // bean.setInvocationNode(null);
                break;
            case "java":
                JavaHelper java = new JavaHelper();
                java.AskJava();
                bean.setVersion(java.getVersion());
                bean.setErrorMessage(java.getErrorMessage());
                bean.setComponentName(component);
                bean.setStatus(java.getStatus());
                break;
            case "scala":
                ScalaHelper scala = new ScalaHelper();
                scala.AskScala();
                bean.setVersion(scala.getVersion());
                bean.setErrorMessage(scala.getErrorMessage());
                bean.setComponentName(component);
                bean.setStatus(scala.getStatus());
                break;
            // }
            // list.add(bean);
        }

        // return list;
        return bean;
    }

    public String[] hostArray(String hostList) {
        return hostList.split(",");
    }

    public String CheckServices(String args) {
        try {
            if (args == null || args.trim().length() == 0) {
                throw new Exception();
            }
            JsonUtility json = new JsonUtility();
            Checker checker = new Checker();
            JSONArray jsonArray = new JSONArray();
            jsonArray = json.GetJsonArray(args);
            // ArrayList<ComponentInfo> list = null;
            ComponentInfo list = null;
            ArrayList<ComponentInfo> finalList = new ArrayList<ComponentInfo>();
            for (int i = 0; i < jsonArray.size(); i++) {
                json.JsonParse(jsonArray.get(i).toString());
                String component = json.GetComponent();
                String hostList = json.GetHostList();
                // String hostArray[] = checker.hostArray(hostList);
                if (component.equalsIgnoreCase("hbase")) {
                    json.ParseOptionalField();
                    String authentication = json.getAuthentication();
                    String masterPrincipal = json.getMasterPrincipal();
                    String regionServer = json.getRegionServer();
                    String keyType = json.getKeyType();
                    String principal = json.getPrincipal();
                    list = checker.CheckHBaseComponent(component, /* hostArray */hostList, authentication,
                            masterPrincipal, regionServer, keyType, principal, json.getNamespace());
                    // System.out.println(authentication);
                    // System.out.println(masterPrincipal);
                    // System.out.println(regionServer);
                    // System.out.println(principal);
                    // System.out.println(keyType);
                } else
                    list = checker.CheckComponent(component, /* hostArray */hostList);
                finalList.add(list);
            }
            return new ObjectMapper().writeValueAsString(finalList);
        } catch (Exception e) {
            e.printStackTrace(new PrintWriter(x));
        }
        return null;
    }

    private /* ArrayList<ComponentInfo> */ ComponentInfo CheckHBaseComponent(String component,
                                                                             String hostArray/* String[] hostArray */, String authentication, String masterPrincipal,
                                                                             String regionServer, String keyType, String principal, String namespace) throws IOException {
        // ArrayList<ComponentInfo> list = new ArrayList<ComponentInfo>();
        HBaseHelper hbase = new HBaseHelper();
        // for (int i = 0; i < hostArray.length; i++) {
        ComponentInfo bean = new ComponentInfo();
        hbase.AskHBase(/* hostArray[i] */hostArray, authentication, masterPrincipal, regionServer, keyType, principal, namespace);
        bean.setVersion(/*hbase.getVersion()*/null);
        bean.setStatus(hbase.getStatus());
        bean.setErrorMessage(hbase.getErrorMessage());
        // bean.setInvocationNode(/* hostArray[i] */null);
        bean.setComponentName(component);
        // }
        return bean;
    }

    private boolean WriteStringToFile(String flName, String str) {
        boolean writtenData = false;
        PrintWriter out = null;
        try {
            out = new PrintWriter(flName, "UTF-8");
            out.print(str);
            writtenData = true;
        } catch (Exception e) {
            System.out.println("Failed to open file:" + flName + "\nUsage: java -jar GetComponent-1.0 <OutputFile> <InputJsonArrayString>");
        } catch (Throwable t) {
            System.out.println("Failed to open file:" + flName + "\nUsage: java -jar GetComponent-1.0 <OutputFile> <InputJsonArrayString>");
        } finally {
            if (out != null)
                out.close();
        }
        return writtenData;
    }

    public static void main(String[] args) {
        /*
        // System.out.println(new Checker().CheckServices(
                // "[{\"component\":\"zookeeper\",\"hostlist\":\"localhost:2181,loclahost:2181\"},{\"component\":\"java\",\"hostlist\":\"192.168.10.20:2181,192.168.10.21:2181\"}]"
                // "[{\"component\":\"hbase\",\"hostlist\":\"localhost\",\"authentication\":\"kerberos\",\"regionserver_principal\":\"hbase/_HOST@INTRANET.LIGADATA.COM\",\"master_principal\":\"hbase/_HOST@INTRANET.LIGADATA.COM\",\"principal\":\"user@INTRANET.LIGADATA.COM\",\"keytab\":\"/apps/kamanja/CertificateInfo/user.keytab\"},{\"component\":\"java\",\"hostlist\":\"localhost\"},{\"component\":\"scala\",\"hostlist\":\"localhost\"},{\"component\":\"zookeeper\",\"hostlist\":\"localhost:2181\"},{\"component\":\"kafka\",\"hostlist\":\"localhost:9092\"}]"));
        */

        if (args.length != 2) {
            System.out.println("Usage: java -jar GetComponent-1.0 <OutputFile> <InputJsonArrayString>");
            System.exit(1);
        }

        Checker checker = new Checker();
        if (checker.WriteStringToFile(args[0], "")) { // To make sure this file write is working
            String componentsInfo = checker.CheckServices(args[1]);
            checker.WriteStringToFile(args[0], componentsInfo);
        }
    }
}
