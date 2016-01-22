/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.Migrate;

import org.apache.logging.log4j.*;

import java.io.File;

import com.ligadata.MigrateBase.*;

import java.util.*;
import java.lang.reflect.Constructor;
// import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import com.google.gson.Gson;
// import com.google.gson.GsonBuilder;
import java.io.FileReader;

/**
 *
 * { "ClusterConfigFile": "", "ApiConfigFile": "", "MigratingFrom": { "Version":
 * "", "VersionInstallPath": "", "ImplemtedClass": "", "Jars": [] },
 * "MigratingTo": { "Version": "", "VersionInstallPath": "", "ImplemtedClass":
 * "", "Jars": [] } }
 *
 */

public class Migrate {
	String loggerName = this.getClass().getName();
	Logger logger = LogManager.getLogger(loggerName);

	class VersionConfig {
		String version = null;
		String versionInstallPath = null;
		String implemtedClass = null;
		List<String> jars = null;

		VersionConfig(String tversion, String tversionInstallPath,
				String timplemtedClass, List<String> tjars) {
			version = tversion;
			versionInstallPath = tversionInstallPath;
			implemtedClass = timplemtedClass;
			jars = tjars;
		}

		VersionConfig() {

		}
	}

	class Configuration {
		String clusterConfigFile = null;
		String apiConfigFile = null;
		String unhandledMetadataDumpDir = null;
		VersionConfig migratingFrom = null;
		VersionConfig migratingTo = null;

		Configuration(String tclusterConfigFile, String tapiConfigFile,
				VersionConfig tmigratingFrom, VersionConfig tmigratingTo) {
			clusterConfigFile = tclusterConfigFile;
			apiConfigFile = tapiConfigFile;
			migratingFrom = tmigratingFrom;
			migratingTo = tmigratingTo;
		}

		Configuration() {

		}
	}

	List<MetadataFormat> allMetadata = new ArrayList<MetadataFormat>();

	class DataCallback implements DataObjectCallBack {
		long cntr = 0;
		MigratableTo migrateTo = null;
		List<DataFormat> collectedData = null;
		int kSaveThreshold = 0;
		String srcVer = "0";
		String dstVer = "0";
		byte[] appendData = new byte[0];

		DataCallback(MigratableTo tmigrateTo, List<DataFormat> tcollectedData,
				int tkSaveThreshold, String tsrcVer, String tdstVer) {
			migrateTo = tmigrateTo;
			collectedData = tcollectedData;
			kSaveThreshold = tkSaveThreshold;
			srcVer = tsrcVer;
			dstVer = tdstVer;

			if (srcVer.equalsIgnoreCase("1.1")
					&& dstVer.equalsIgnoreCase("1.3")) {
				appendData = new byte[8]; // timepartition bytes at the end.
				for (int i = 0; i < 8; i++)
					appendData[0] = 0;
			}
		}

		public boolean call(DataFormat[] objData) throws Exception {
			for (DataFormat d : objData) {
				int readLen = d.data.length;
				if (appendData.length > 0) {
					byte[] result = new byte[d.data.length + appendData.length];
					System.arraycopy(d.data, 0, result, 0, d.data.length);
					System.arraycopy(appendData, 0, result, d.data.length,
							appendData.length);
					d.data = result;
				}
				int writeLen = d.data.length;
				logger.debug(String
						.format("cntr:%d, Container:%s, TimePartitionValue:%d, BucketKey:%s, TxnId:%d, RowId:%d, SerializerName:%s, DataSize:(Read:%d, Write:%d)",
								cntr, d.containerName, d.timePartition,
								Arrays.toString(d.bucketKey), d.transactionid,
								d.rowid, d.serializername, readLen, writeLen));
				collectedData.add(d);
				cntr += 1;
			}
			if (collectedData.size() >= kSaveThreshold) {
				migrateTo.populateAndSaveData(collectedData
						.toArray(new DataFormat[collectedData.size()]));
				collectedData.clear();
			}
			return true;
		}
	}

	class MdCallback implements MetadataObjectCallBack {
		public boolean call(MetadataFormat objData) throws Exception {
			logger.debug(String.format("Got Metadata => Key:%s, JsonString:%s",
					objData.objType, objData.objDataInJson));
			allMetadata.add(objData);
			return true;
		}
	}

	Configuration GetConfigurationFromCfgFile(String cfgfile) {
		FileReader reader = null;
		try {
			reader = new FileReader(cfgfile);
			try {
				Gson gson = new Gson();
				Configuration cfg = gson.fromJson(reader, Configuration.class);
				logger.debug("Populated migrate configuration:"
						+ gson.toJson(cfg));
				return cfg;
			} catch (Exception e) {
			} catch (Throwable e) {
			} finally {

			}
			return null;
		} catch (Exception e) {
		} catch (Throwable e) {
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
		}
		return null;

		/*
		 * VersionConfig fromCfg = new VersionConfig("1.1",
		 * "/data/Kamanja_1.1.3", "com.ligadata.Migrate.MigrateFrom_V_1_1", new
		 * String[] { "/tmp/Migrate/Migrate/migratebase-1.0.jar",
		 * "/tmp/Migrate/Migrate/migratefrom_v_1_1_2.10-1.0.jar",
		 * "/data/Kamanja_1.1.3/bin/KamanjaManager-1.0" }); VersionConfig toCfg
		 * = new VersionConfig( "1.3", "/data/Kamanja",
		 * "com.ligadata.Migrate.MigrateTo_V_1_3", new String[] {
		 * "/tmp/Migrate/Migrate/migratebase-1.0.jar",
		 * "/tmp/Migrate/Migrate/migrateto_v_1_3_2.11-1.0.jar",
		 * "/data/Kamanja/lib/system/SaveContainerDataComponent-1.0" });
		 * Configuration cfg = new Configuration(
		 * "/tmp/Migrate/config/ClusterConfig.json",
		 * "/tmp/Migrate/config/MetadataAPIConfig.properties", fromCfg, toCfg);
		 * return cfg;
		 */

		/*
		 * 
		 * val cfgStr = Source.fromFile(cfgfile).mkString
		 * 
		 * var configMap: Map[String, Any] = null
		 * 
		 * try { implicit val jsonFormats = DefaultFormats val json =
		 * parse(cfgStr) logger.debug("Valid json: " + cfgStr)
		 * 
		 * configMap = json.values.asInstanceOf[Map[String, Any]] } catch { case
		 * e: Exception => { logger.error(
		 * "Failed to parse JSON from input config file:%s.\nInvalid JSON:%s"
		 * .format(cfgfile, cfgStr), e) throw e } }
		 * 
		 * val errSb = new StringBuilder
		 * 
		 * val clusterCfgFile = configMap.getOrElse("ClusterConfigFile",
		 * "").toString.trim if (clusterCfgFile.size == 0) {
		 * errSb.append("Not found valid ClusterConfigFile key in Configfile:%s\n"
		 * .format(cfgfile)) } else { val tmpfl = new File(clusterCfgFile) if
		 * (tmpfl.exists == false || tmpfl.isFile == false) {
		 * errSb.append("Not found valid ClusterConfigFile key in Configfile:%s\n"
		 * .format(cfgfile)) } }
		 * 
		 * val apiCfgFile = configMap.getOrElse("ApiConfigFile",
		 * "").toString.trim if (apiCfgFile.size == 0) {
		 * errSb.append("Not found valid ApiConfigFile key in Configfile:%s\n"
		 * .format(cfgfile)) } else { val tmpfl = new File(clusterCfgFile) if
		 * (tmpfl.exists == false || tmpfl.isFile == false) {
		 * errSb.append("Not found valid ApiConfigFile key in Configfile:%s\n"
		 * .format(cfgfile)) } }
		 * 
		 * var migrateFromConfig: MigrateFromConfig = null val migrateFrom =
		 * configMap.getOrElse("MigratingFrom", null) if (migrateFrom == null) {
		 * errSb
		 * .append("Not found valid MigratingFrom key in Configfile:%s\n".format
		 * (cfgfile)) } else { var fromMap: Map[String, Any] = null try {
		 * fromMap = migrateFrom.asInstanceOf[Map[String, Any]] } catch { case
		 * e: Exception => {
		 * errSb.append("Not found valid MigratingFrom key in Configfile:%s\n"
		 * .format (cfgfile)) } case e: Throwable => {
		 * errSb.append("Not found valid MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } }
		 * 
		 * if (fromMap != null) { val version = fromMap.getOrElse("Version",
		 * "").toString.trim if (version.size == 0) { errSb.append(
		 * "Not found valid Version of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) }
		 * 
		 * val versionInstallPath = fromMap.getOrElse("VersionInstallPath",
		 * "").toString.trim if (versionInstallPath.size == 0) { errSb.append(
		 * "Not found valid VersionInstallPath of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } else { val tmpfl = new File(versionInstallPath)
		 * if (tmpfl.exists == false || tmpfl.isDirectory == false) {
		 * errSb.append(
		 * "Not found valid VersionInstallPath of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } }
		 * 
		 * val implemtedClass = fromMap.getOrElse("ImplemtedClass",
		 * "").toString.trim if (implemtedClass.size == 0) { errSb.append(
		 * "Not found valid ImplemtedClass of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) }
		 * 
		 * var jars: List[String] = null val tjars = fromMap.getOrElse("Jars",
		 * null) if (tjars == null) { errSb.append(
		 * "Not found valid Jars of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } else { try { jars =
		 * tjars.asInstanceOf[List[String]] } catch { case e: Exception => {
		 * errSb
		 * .append("Not found valid Jars of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } case e: Throwable => { errSb.append(
		 * "Not found valid Jars of MigratingFrom key in Configfile:%s\n"
		 * .format(cfgfile)) } } } migrateFromConfig =
		 * MigrateFromConfig(version, versionInstallPath, implemtedClass, jars)
		 * } }
		 * 
		 * var migrateToConfig: MigrateToConfig = null val migrateTo =
		 * configMap.getOrElse("MigratingTo", null) if (migrateTo == null) {
		 * errSb.append
		 * ("Not found valid MigratingTo key in Configfile:%s\n".format
		 * (cfgfile)) } else { var toMap: Map[String, Any] = null try { toMap =
		 * migrateTo.asInstanceOf[Map[String, Any]] } catch { case e: Exception
		 * => { errSb
		 * .append("Not found valid MigratingTo key in Configfile:%s\n".format(
		 * cfgfile )) } case e: Throwable => {
		 * errSb.append("Not found valid MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) } }
		 * 
		 * if (toMap != null) { val version = toMap.getOrElse("Version",
		 * "").toString.trim if (version.size == 0) { errSb.append(
		 * "Not found valid Version of MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) }
		 * 
		 * val implemtedClass = toMap.getOrElse("ImplemtedClass",
		 * "").toString.trim if (implemtedClass.size == 0) { errSb.append(
		 * "Not found valid ImplemtedClass of MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) }
		 * 
		 * var jars: List[String] = null val tjars = toMap.getOrElse("Jars",
		 * null) if (tjars == null) {
		 * errSb.append("Not found valid Jars of MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) } else { try { jars =
		 * tjars.asInstanceOf[List[String]] } catch { case e: Exception => {
		 * errSb
		 * .append("Not found valid Jars of MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) } case e: Throwable => {
		 * errSb.append("Not found valid Jars of MigratingTo key in Configfile:%s\n"
		 * .format(cfgfile)) } } } migrateToConfig = MigrateToConfig(version,
		 * implemtedClass, jars) } }
		 * 
		 * if (errSb.size > 0) { logger.error(errSb.toString) sys.exit(1) }
		 * 
		 * Configuration(clusterCfgFile, apiCfgFile, migrateFromConfig,
		 * migrateToConfig)
		 */
	}

	void usage() {
		logger.warn("Usage: migrate --config <ConfigurationJsonFile>");
	}

	boolean isValidPath(String path, boolean checkForDir, boolean checkForFile,
			String str) {
		File fl = new File(path);
		if (fl.exists() == false) {
			logger.error("Given " + str + ":" + path + " does not exists");
			return false;
		}

		if (checkForDir && fl.isDirectory() == false) {
			logger.error("Given " + str + ":" + path + " is not directory");
			return false;
		}

		if (checkForFile && fl.isFile() == false) {
			logger.error("Given " + str + ":" + path + " is not file");
			return false;
		}

		return true;
	}

	public void run(String[] args) {
		MigratableFrom migrateFrom = null;
		MigratableTo migrateTo = null;
		URLClassLoader srcKamanjaLoader = null;
		URLClassLoader dstKamanjaLoader = null;

		try {
			if (args.length != 2) {
				usage();
				return;
			}

			String backupTblSufix = ".bak";

			String cfgfile = "";
			if (args[0].equalsIgnoreCase("--config")) {
				cfgfile = args[1].trim();
			} else {
				logger.error("Unknown option " + args[0]);
				usage();
				System.exit(1);
			}

			if (cfgfile.length() == 0) {
				logger.error("Input required config file");
				usage();
				System.exit(1);
			}

			if (isValidPath(cfgfile, false, true, "ConfigFile") == false) {
				usage();
				System.exit(1);
			}

			Configuration configuration = GetConfigurationFromCfgFile(cfgfile);

			if (configuration == null) {
				logger.error("Failed to get configuration from given file:"
						+ cfgfile);
				usage();
				System.exit(1);
			}

			// Version check

			String srcVer = configuration.migratingFrom.version.trim();
			String dstVer = configuration.migratingTo.version.trim();

			if (srcVer.equalsIgnoreCase("1.1") == false
					&& srcVer.equalsIgnoreCase("1.2") == false) {
				logger.error("We support source versions only 1.1 or 1.2. We don't support "
						+ srcVer);
				usage();
				System.exit(1);
			}

			if (dstVer.equalsIgnoreCase("1.3") == false) {
				logger.error("We support destination version only 1.3. We don't support "
						+ srcVer);
				usage();
				System.exit(1);
			}

			int srcJarsCnt = configuration.migratingFrom.jars.size();
			URL[] srcLoaderUrls = new URL[srcJarsCnt];

			int idx = 0;
			for (String jar : configuration.migratingFrom.jars) {
				logger.debug("Migration From URL => " + jar);
				srcLoaderUrls[idx++] = new File(jar).toURI().toURL();
			}

			srcKamanjaLoader = new URLClassLoader(srcLoaderUrls);

			Class<?> srcClass = srcKamanjaLoader
					.loadClass(configuration.migratingFrom.implemtedClass);

			Constructor<?> srcConstructor = srcClass.getConstructor();
			Object tmpSrcObj = srcConstructor.newInstance();
			if (tmpSrcObj instanceof MigratableFrom) {
				migrateFrom = (MigratableFrom) tmpSrcObj;
			} else {
				logger.error(String
						.format("Failed to Load Source. Version:%s, migrateFromClass:%s, InstallPath:%s, ",
								configuration.migratingFrom.version,
								configuration.migratingFrom.implemtedClass,
								configuration.migratingFrom.versionInstallPath));
			}

			int dstJarsCnt = configuration.migratingTo.jars.size();
			URL[] dstLoaderUrls = new URL[dstJarsCnt];

			idx = 0;
			for (String jar : configuration.migratingTo.jars) {
				logger.debug("Migration To URL => " + jar);
				dstLoaderUrls[idx++] = new File(jar).toURI().toURL();
			}

			dstKamanjaLoader = new URLClassLoader(dstLoaderUrls);

			Class<?> dstClass = dstKamanjaLoader
					.loadClass(configuration.migratingTo.implemtedClass);

			Constructor<?> dstConstructor = dstClass.getConstructor();
			Object tmpDstObj = dstConstructor.newInstance();
			if (tmpDstObj instanceof MigratableTo) {
				migrateTo = (MigratableTo) tmpDstObj;
			} else {
				logger.error(String
						.format("Failed to Load Destination. Version:%s, migrateToClass:%s",
								configuration.migratingTo.version,
								configuration.migratingTo.implemtedClass));
			}

			logger.debug(String.format(
					"apiConfigFile:%s, clusterConfigFile:%s",
					configuration.apiConfigFile,
					configuration.clusterConfigFile));
			migrateTo.init(configuration.migratingTo.versionInstallPath,
					configuration.apiConfigFile,
					configuration.clusterConfigFile, srcVer,
					configuration.unhandledMetadataDumpDir);

			String metadataStoreInfo = migrateTo.getMetadataStoreInfo();
			String dataStoreInfo = migrateTo.getDataStoreInfo();
			String statusStoreInfo = migrateTo.getStatusStoreInfo();

			logger.debug(String
					.format("metadataStoreInfo:%s, dataStoreInfo:%s, statusStoreInfo:%s",
							metadataStoreInfo, dataStoreInfo, statusStoreInfo));
			migrateFrom.init(configuration.migratingFrom.versionInstallPath,
					metadataStoreInfo, dataStoreInfo, statusStoreInfo);

			String[] allMetadataTbls = migrateFrom.getAllMetadataTableNames();
			String[] allDataTbls = migrateFrom.getAllDataTableNames();
			String[] allStatusTbls = migrateFrom.getAllStatusTableNames();

			List<BackupTableInfo> metadataBackupTbls = new ArrayList<BackupTableInfo>();
			List<BackupTableInfo> dataBackupTbls = new ArrayList<BackupTableInfo>();
			List<BackupTableInfo> statusBackupTbls = new ArrayList<BackupTableInfo>();

			List<String> metadataDelTbls = new ArrayList<String>();
			List<String> dataDelTbls = new ArrayList<String>();
			List<String> statusDelTbls = new ArrayList<String>();

			boolean allTblsBackedUp = true;

			for (String tbl : allMetadataTbls) {
				BackupTableInfo bkup = new BackupTableInfo(tbl, tbl
						+ backupTblSufix);
				metadataBackupTbls.add(bkup);
				metadataDelTbls.add(tbl);

				if (migrateTo.isMetadataTableExists(tbl)
						&& migrateTo.isMetadataTableExists(bkup.dstTable) == false) {
					allTblsBackedUp = false;
				}
			}

			for (String tbl : allDataTbls) {
				BackupTableInfo bkup = new BackupTableInfo(tbl, tbl
						+ backupTblSufix);
				dataBackupTbls.add(bkup);
				dataDelTbls.add(tbl);

				if (migrateTo.isDataTableExists(tbl)
						&& migrateTo.isDataTableExists(bkup.dstTable) == false) {
					allTblsBackedUp = false;
				}
			}

			for (String tbl : allStatusTbls) {
				BackupTableInfo bkup = new BackupTableInfo(tbl, tbl
						+ backupTblSufix);
				statusBackupTbls.add(bkup);
				statusDelTbls.add(tbl);

				if (migrateTo.isStatusTableExists(tbl)
						&& migrateTo.isStatusTableExists(bkup.dstTable) == false) {
					allTblsBackedUp = false;
				}
			}

			// Backup all the tables, if any one of them is missing
			if (allTblsBackedUp == false) {
				migrateTo.backupMetadataTables(
						metadataBackupTbls
								.toArray(new BackupTableInfo[metadataBackupTbls
										.size()]), true);
				migrateTo.backupDataTables(dataBackupTbls
						.toArray(new BackupTableInfo[dataBackupTbls.size()]),
						true);
				migrateTo.backupStatusTables(statusBackupTbls
						.toArray(new BackupTableInfo[statusBackupTbls.size()]),
						true);
			}

			// Drop all tables after backup
			migrateTo.dropMetadataTables(metadataDelTbls
					.toArray(new String[metadataDelTbls.size()]));
			migrateTo.dropDataTables(dataDelTbls.toArray(new String[dataDelTbls
					.size()]));
			migrateTo.dropStatusTables(statusDelTbls
					.toArray(new String[statusDelTbls.size()]));

			migrateFrom.getAllMetadataObjs(backupTblSufix, new MdCallback());

			migrateTo.uploadConfiguration();

			MetadataFormat[] metadataArr = allMetadata
					.toArray(new MetadataFormat[allMetadata.size()]);

			migrateTo.addMetadata(metadataArr);

			int kSaveThreshold = 10000;

			List<DataFormat> collectedData = new ArrayList<DataFormat>();

			DataCallback dataCallback = new DataCallback(migrateTo,
					collectedData, kSaveThreshold, srcVer, dstVer);

			migrateFrom.getAllDataObjs(backupTblSufix, metadataArr,
					dataCallback);

			if (collectedData.size() > 0) {
				migrateTo.populateAndSaveData(collectedData
						.toArray(new DataFormat[collectedData.size()]));
				collectedData.clear();
			}

			logger.info("Migration is done");
			System.out.println("Migration is done");
		} catch (Exception e) {
			logger.error("Failed to Migrate", e);
		} catch (Throwable t) {
			logger.error("Failed to Migrate", t);
		} finally {
			if (migrateFrom != null)
				migrateFrom.shutdown();
			if (migrateTo != null)
				migrateTo.shutdown();
			try {
				if (srcKamanjaLoader != null)
					srcKamanjaLoader.close();
			} catch (Exception e) {
			} catch (Throwable t) {
			}

			try {
				if (dstKamanjaLoader != null)
					dstKamanjaLoader.close();
			} catch (Exception e) {
			} catch (Throwable t) {
			}
		}
	}

	public static void main(String[] args) {
		new Migrate().run(args);
	}
}
