KAMANJA_HOME={InstallDirectory}

java -jar $KAMANJA_HOME/bin/KVInit-1.0 --kvname System.SputumCodes        --config $KAMANJA_HOME/config/Engine1Config.properties --csvpath $KAMANJA_HOME/input/SampleApplications/data/sputumCodes_Medical.csv       --keyfieldname icd9Code
java -jar $KAMANJA_HOME/bin/KVInit-1.0 --kvname System.SmokeCodes         --config $KAMANJA_HOME/config/Engine1Config.properties --csvpath $KAMANJA_HOME/input/SampleApplications/data/smokingCodes_Medical.csv      --keyfieldname icd9Code
java -jar $KAMANJA_HOME/bin/KVInit-1.0 --kvname System.EnvCodes           --config $KAMANJA_HOME/config/Engine1Config.properties --csvpath $KAMANJA_HOME/input/SampleApplications/data/envExposureCodes_Medical.csv  --keyfieldname icd9Code
java -jar $KAMANJA_HOME/bin/KVInit-1.0 --kvname System.CoughCodes         --config $KAMANJA_HOME/config/Engine1Config.properties --csvpath $KAMANJA_HOME/input/SampleApplications/data/coughCodes_Medical.csv        --keyfieldname icd9Code
java -jar $KAMANJA_HOME/bin/KVInit-1.0 --kvname System.DyspnoeaCodes      --config $KAMANJA_HOME/config/Engine1Config.properties --csvpath $KAMANJA_HOME/input/SampleApplications/data/dyspnoea_Medical.csv          --keyfieldname icd9Code
