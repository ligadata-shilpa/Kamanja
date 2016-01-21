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

package com.ligadata.MigrateBase;

public interface MigratableTo {
  public abstract void init(String destInstallPath, String apiConfigFile, String clusterConfigFile, String sourceVersion, String unhandledMetadataDumpDir); // Source version is like 1.1 or 1.2, etc
  public abstract boolean isInitialized();
  public abstract String getMetadataStoreInfo();
  public abstract String getDataStoreInfo();
  public abstract String getStatusStoreInfo();
  public abstract boolean isMetadataTableExists(String tblName);
  public abstract boolean isDataTableExists(String tblName);
  public abstract boolean isStatusTableExists(String tblName);
  public abstract void backupMetadataTables(BackupTableInfo[] tblsToBackedUp, boolean force);
  public abstract void backupDataTables(BackupTableInfo[] tblsToBackedUp, boolean force);
  public abstract void backupStatusTables(BackupTableInfo[] tblsToBackedUp, boolean force);
  public abstract void dropMetadataTables(String[] tblsToDrop);
  public abstract void dropDataTables(String[] tblsToDrop);
  public abstract void dropStatusTables(String[] tblsToDrop);
  public abstract void uploadConfiguration();
  public abstract void addMetadata(MetadataFormat[] allMetadataElemsJson);
  public abstract void populateAndSaveData(DataFormat[] data);
  public abstract void shutdown();
}

