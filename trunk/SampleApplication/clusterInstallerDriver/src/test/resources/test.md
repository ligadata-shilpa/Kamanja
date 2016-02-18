    val clusterId : String = if (options.contains('clusterId)) options.apply('clusterId) else null
    val apiConfigPath : String = if (options.contains('apiConfig)) options.apply('apiConfig) else null
    val nodeConfigPath : String = if (options.contains('clusterConfig)) options.apply('clusterConfig) else null
    val tarballPath : String = if (options.contains('tarballPath)) options.apply('tarballPath) else null
    val fromKamanja : String = if (options.contains('fromKamanja)) options.apply('fromKamanja) else null
    val fromScala : String = if (options.contains('fromScala)) options.apply('fromScala) else "2.10"
    val toScala : String = if (options.contains('toScala)) options.apply('toScala) else "2.11"
    val workingDir : String = if (options.contains('workingDir)) options.apply('workingDir) else null
    val upgrade : Boolean = if (options.contains('upgrade)) options.apply('upgrade) == "true" else false
    val install : Boolean = if (options.contains('install)) options.apply('install) == "true" else false



export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/KAMANJA-336/kamanja/trunk
cd $KAMANJA_HOME

java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_HOME/clusterInstallerDriver-1.0 
                --clusterId "kamanjacluster1"
                --apiConfig "/tmp/drdigital/config/MetadataAPIConfig.properties"
                --clusterConfig "/tmp/drdigital/config/ClusterConfig.json"
                --tarballPath "/tmp/drdigital.tar.gz"
                --fromKamanja "1.1"
                --fromScala "2.10"
                --toScala "2.11"
                --workingDir /tmp/work
                --upgrade 

