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

public class Migrate {
    String loggerName = this.getClass().getName();
    Logger logger = LogManager.getLogger(loggerName);

    class VersionConfig {
        String version = null;
        String scalaVersion = null;
        String versionInstallPath = null;
        String implemtedClass = null;
        List<String> jars = null;

        VersionConfig() {
        }
    }

    class Configuration {
        int dataSaveThreshold = 1000;
        String clusterConfigFile = null;
        String apiConfigFile = null;
        String unhandledMetadataDumpDir = null;
        VersionConfig migratingFrom = null;
        VersionConfig migratingTo = null;
        List<String> excludeMetadata = null;
        boolean excludeData = false;
        int parallelDegree = 0;
        boolean mergeContainersAndMessages = true;

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

                if ((d.containerName.equalsIgnoreCase("AdapterUniqKvData"))
                        || (d.containerName.equalsIgnoreCase("GlobalCounters"))
                        || (d.containerName.equalsIgnoreCase("ModelResults"))) {
                    // Don't change any data
                } else {
                    if (appendData.length > 0) {
                        byte[] result = new byte[d.data.length
                                + appendData.length];
                        System.arraycopy(d.data, 0, result, 0, d.data.length);
                        System.arraycopy(appendData, 0, result, d.data.length,
                                appendData.length);
                        d.data = result;
                    }
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
                logger.error("", e);
            } catch (Throwable e) {
                logger.error("", e);
            } finally {

            }
            return null;
        } catch (Exception e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (Exception e) {
            }
        }
        return null;
    }

    Configuration GetConfigurationFromCfgJsonString(String cfgString) {
        try {
            Gson gson = new Gson();
            Configuration cfg = gson.fromJson(cfgString, Configuration.class);
            logger.debug("Populated migrate configuration:"
                    + gson.toJson(cfg));
            return cfg;
        } catch (Exception e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        }
        return null;
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

    public int runFromArgs(String[] args) {
        try {
            if (args.length != 2) {
                usage();
                return 1;
            }

            String cfgfile = "";
            if (args[0].equalsIgnoreCase("--config")) {
                cfgfile = args[1].trim();
            } else {
                logger.error("Unknown option " + args[0]);
                usage();
                return 1;
            }

            if (cfgfile.length() == 0) {
                logger.error("Input required config file");
                usage();
                return 1;
            }

            if (isValidPath(cfgfile, false, true, "ConfigFile") == false) {
                usage();
                return 1;
            }

            Configuration configuration = GetConfigurationFromCfgFile(cfgfile);

            if (configuration == null) {
                logger.error("Failed to get configuration from given file:"
                        + cfgfile);
                usage();
                return 1;
            }

            return run(configuration);
        } catch (Exception e) {
            logger.error("Failed to Migrate", e);
        } catch (Throwable t) {
            logger.error("Failed to Migrate", t);
        }
        return 1;
    }

    public int runFromJsonConfigString(String jsonConfigString) {
        try {
            if (jsonConfigString.length() == 0) {
                logger.error("Passed invalid json string");
                usage();
                return 1;
            }

            Configuration configuration = GetConfigurationFromCfgJsonString(jsonConfigString);

            if (configuration == null) {
                logger.error("Failed to get configuration from given JSON String:" + jsonConfigString);
                usage();
                return 1;
            }

            return run(configuration);
        } catch (Exception e) {
            logger.error("Failed to Migrate", e);
        } catch (Throwable t) {
            logger.error("Failed to Migrate", t);
        }
        return 1;
    }

    private int run(Configuration configuration) {
        MigratableFrom migrateFrom = null;
        MigratableTo migrateTo = null;
        URLClassLoader srcKamanjaLoader = null;
        URLClassLoader dstKamanjaLoader = null;
        int retCode = 1;
        boolean foundError = false;

        try {
            if (configuration == null) {
                logger.error("Found invalid configuration");
                usage();
                return retCode;
            }

            // Version check
            String srcVer = configuration.migratingFrom.version.trim();
            String dstVer = configuration.migratingTo.version.trim();
            String scalaFrom = configuration.migratingFrom.scalaVersion.trim();
            String scalaTo = configuration.migratingTo.scalaVersion.trim();

            if (srcVer.equalsIgnoreCase("1.1") == false
                    && srcVer.equalsIgnoreCase("1.2") == false) {
                logger.error("We support source versions only 1.1 or 1.2. We don't support "
                        + srcVer);
                usage();
                return retCode;
            }

            if (dstVer.equalsIgnoreCase("1.3") == false) {
                logger.error("We support destination version only 1.3. We don't support "
                        + srcVer);
                usage();
                return retCode;
            }

            if (scalaFrom.equalsIgnoreCase("2.10") == false /* && scalaFrom.equalsIgnoreCase("2.11") == false */) {
                logger.error("We support source scala version only 2.10. Given:" + scalaFrom);
                usage();
                return retCode;
            }

            if (scalaTo.equalsIgnoreCase("2.10") == false && scalaTo.equalsIgnoreCase("2.11") == false) {
                logger.error("We support destination scala version only 2.10 or 2.11. Given:" + scalaTo);
                usage();
                return retCode;
            }

            String backupTblSufix = "_migrate_" + srcVer.replace('.', '_').trim() + "_to_" + dstVer.replace('.', '_').trim() + "_bak";

            String curMigrationUnhandledMetadataDumpDir = "";
            String curMigrationSummary = "";
            String sourceReadFailuresFilePath = "";

            if (isValidPath(configuration.unhandledMetadataDumpDir, true,
                    false, "unhandledMetadataDumpDir")) {
                String dirExtn = new java.text.SimpleDateFormat(
                        "yyyyMMddHHmmss").format(new java.util.Date(System
                        .currentTimeMillis()));

                String newDir = configuration.unhandledMetadataDumpDir
                        + "/Migrate_" + dirExtn;

                File fl = new File(newDir);

                if (fl.mkdir()) {
                    curMigrationUnhandledMetadataDumpDir = fl.getAbsolutePath();
                    curMigrationSummary = configuration.unhandledMetadataDumpDir
                            + "/MigrateSummary_" + dirExtn + ".log";
                    sourceReadFailuresFilePath = configuration.unhandledMetadataDumpDir
                            + "/ReadFailures_" + dirExtn + ".err";
                    logger.info("Current Migration Changes are writing into "
                            + curMigrationUnhandledMetadataDumpDir
                            + " directory. And summary is writing into "
                            + curMigrationSummary
                            + " and source read failures are writing into "
                            + sourceReadFailuresFilePath);
                } else {
                    logger.error("Failed to create directory " + newDir);
                    usage();
                    return retCode;
                }
            } else {
                usage();
                return retCode;
            }

            if (srcVer.equalsIgnoreCase("1.2") &&
                    dstVer.equalsIgnoreCase("1.3") &&
                    scalaFrom.equalsIgnoreCase("2.10") &&
                    scalaTo.equalsIgnoreCase("2.10")) {
                logger.warn("Nothing to migrate from 1.2 to 1.3 with scala 2.10 version");
                return 0;
            }

            // From Srouce version 1.1 to Destination version 1.3 we do both
            // Metadata Upgrade & Data Upgrade
            // From Source Version 1.2 to Destination version 1.3, we only do
            // Metadata Upgrade.
            boolean canUpgradeMetadata = ((srcVer.equalsIgnoreCase("1.1") || srcVer
                    .equalsIgnoreCase("1.2")) && dstVer.equalsIgnoreCase("1.3"));
            boolean canUpgradeData = (srcVer.equalsIgnoreCase("1.1") && dstVer
                    .equalsIgnoreCase("1.3"));

            if (canUpgradeData && canUpgradeMetadata == false) {
                logger.error("We don't support upgrading only data without metadata at this moment");
                usage();
                return retCode;
            }

            // Modify canUpgradeData depending on exclude flag
            canUpgradeData = (canUpgradeData && configuration.excludeData == false);

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
                foundError = true;
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
                foundError = true;
            }

            if (foundError == false) {
                logger.debug(String.format(
                        "apiConfigFile:%s, clusterConfigFile:%s",
                        configuration.apiConfigFile,
                        configuration.clusterConfigFile));
                migrateTo.init(configuration.migratingTo.versionInstallPath,
                        configuration.apiConfigFile,
                        configuration.clusterConfigFile,
                        srcVer,
                        curMigrationUnhandledMetadataDumpDir,
                        curMigrationSummary,
                        configuration.parallelDegree,
                        configuration.mergeContainersAndMessages,
                        scalaFrom,
                        scalaTo);

                String metadataStoreInfo = migrateTo.getMetadataStoreInfo();
                String dataStoreInfo = migrateTo.getDataStoreInfo();
                String statusStoreInfo = migrateTo.getStatusStoreInfo();

                logger.debug(String
                        .format("metadataStoreInfo:%s, dataStoreInfo:%s, statusStoreInfo:%s",
                                metadataStoreInfo, dataStoreInfo, statusStoreInfo));
                migrateFrom.init(configuration.migratingFrom.versionInstallPath,
                        metadataStoreInfo, dataStoreInfo, statusStoreInfo,
                        sourceReadFailuresFilePath);

                TableName[] allMetadataTbls = new TableName[0];
                TableName[] allDataTbls = new TableName[0];
                TableName[] allStatusTbls = new TableName[0];

                if (canUpgradeMetadata)
                    allMetadataTbls = migrateFrom.getAllMetadataTableNames();
                if (canUpgradeData) {
                    allDataTbls = migrateFrom.getAllDataTableNames();
                    allStatusTbls = migrateFrom.getAllStatusTableNames();
                }

                List<BackupTableInfo> metadataBackupTbls = new ArrayList<BackupTableInfo>();
                List<BackupTableInfo> dataBackupTbls = new ArrayList<BackupTableInfo>();
                List<BackupTableInfo> statusBackupTbls = new ArrayList<BackupTableInfo>();

                List<TableName> metadataDelTbls = new ArrayList<TableName>();
                List<TableName> dataDelTbls = new ArrayList<TableName>();
                List<TableName> statusDelTbls = new ArrayList<TableName>();

                boolean allTblsBackedUp = true;
                int foundMdTablesWhichBackedUp = 0;
                int foundMdTablesWhichDidnotBackedUp = 0;

                for (TableName tblInfo : allMetadataTbls) {
                    BackupTableInfo bkup = new BackupTableInfo(tblInfo.namespace,
                            tblInfo.name, tblInfo.name + backupTblSufix);

                    if (migrateTo.isMetadataTableExists(tblInfo)) {
                        if (migrateTo.isMetadataTableExists(new TableName(
                                tblInfo.namespace, bkup.dstTable)) == false) {
                            allTblsBackedUp = false;
                        }
                        metadataBackupTbls.add(bkup);
                        metadataDelTbls.add(tblInfo);
                    } else {
                        // If main table does not exists and backup table does not exists mean there is some issue with getting tables or something like that
                        if (migrateTo.isMetadataTableExists(new TableName(
                                tblInfo.namespace, bkup.dstTable))) {
                            foundMdTablesWhichBackedUp += 1;
                        } else {
                            foundMdTablesWhichDidnotBackedUp += 1;
                        }
                    }
                }

                if (foundMdTablesWhichDidnotBackedUp > 0 && foundMdTablesWhichBackedUp == 0) {
                    // Not really found tables to backup
                    throw new Exception("Did not find any metadata table and also not found any backed up tables.");
                }

                for (TableName tblInfo : allDataTbls) {
                    BackupTableInfo bkup = new BackupTableInfo(tblInfo.namespace,
                            tblInfo.name, tblInfo.name + backupTblSufix);
                    if (migrateTo.isDataTableExists(tblInfo)) {
                        if (migrateTo.isDataTableExists(new TableName(
                                tblInfo.namespace, bkup.dstTable)) == false) {
                            allTblsBackedUp = false;
                        }
                        dataBackupTbls.add(bkup);
                        dataDelTbls.add(tblInfo);
                    }

                }

                for (TableName tblInfo : allStatusTbls) {
                    BackupTableInfo bkup = new BackupTableInfo(tblInfo.namespace,
                            tblInfo.name, tblInfo.name + backupTblSufix);
                    if (migrateTo.isStatusTableExists(tblInfo)) {
                        if (migrateTo.isStatusTableExists(new TableName(
                                tblInfo.namespace, bkup.dstTable)) == false) {
                            allTblsBackedUp = false;
                        }
                        statusBackupTbls.add(bkup);
                        statusDelTbls.add(tblInfo);
                    }
                }

                // Backup all the tables, if any one of them is missing
                if (allTblsBackedUp == false) {
/*
                    if (metadataBackupTbls.size() > 0)
                        migrateTo.backupMetadataTables(metadataBackupTbls
                                .toArray(new BackupTableInfo[metadataBackupTbls
                                        .size()]), true);
                    if (dataBackupTbls.size() > 0)
                        migrateTo.backupDataTables(
                                dataBackupTbls
                                        .toArray(new BackupTableInfo[dataBackupTbls
                                                .size()]), true);
                    if (statusBackupTbls.size() > 0)
                        migrateTo.backupStatusTables(statusBackupTbls
                                .toArray(new BackupTableInfo[statusBackupTbls
                                        .size()]), true);
*/
                    migrateTo.backupAllTables(metadataBackupTbls.toArray(new BackupTableInfo[metadataBackupTbls.size()]),
                            dataBackupTbls.toArray(new BackupTableInfo[dataBackupTbls.size()]),
                            statusBackupTbls.toArray(new BackupTableInfo[statusBackupTbls.size()]), true);
                }

                // Drop all tables after backup
                if (metadataDelTbls.size() > 0)
                    migrateTo.dropMetadataTables(metadataDelTbls
                            .toArray(new TableName[metadataDelTbls.size()]));
                if (dataDelTbls.size() > 0)
                    migrateTo.dropDataTables(dataDelTbls
                            .toArray(new TableName[dataDelTbls.size()]));
                if (statusDelTbls.size() > 0)
                    migrateTo.dropStatusTables(statusDelTbls
                            .toArray(new TableName[statusDelTbls.size()]));

                String[] excludeMetadata = new String[0];
                if (configuration.excludeMetadata != null
                        && configuration.excludeMetadata.size() > 0) {
                    excludeMetadata = configuration.excludeMetadata
                            .toArray((new String[configuration.excludeMetadata
                                    .size()]));
                }

                if (canUpgradeMetadata) {
                    migrateFrom.getAllMetadataObjs(backupTblSufix,
                            new MdCallback(), excludeMetadata);
                }

                MetadataFormat[] metadataArr = allMetadata
                        .toArray(new MetadataFormat[allMetadata.size()]);

                if (canUpgradeData)
                    migrateTo.dropMessageContainerTablesFromMetadata(metadataArr);

                if (canUpgradeMetadata)
                    migrateTo.addMetadata(metadataArr, true, excludeMetadata);

                if (canUpgradeData) {
                    int kSaveThreshold = 1000;

                    if (configuration.dataSaveThreshold > 0)
                        kSaveThreshold = configuration.dataSaveThreshold;

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
                }

                logger.info("Migration is done. Failed summary is written to "
                        + curMigrationSummary);
                if (logger.isInfoEnabled() == false)
                    System.out
                            .println("Migration is done. Failed summary is written to "
                                    + curMigrationSummary
                                    + " and failed to read data written to "
                                    + sourceReadFailuresFilePath);
                retCode = 0;
            }
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
        return retCode;
    }

    public static void main(String[] args) {
        System.exit(new Migrate().runFromArgs(args));
    }
}

