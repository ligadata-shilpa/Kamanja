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

package com.ligadata.jpmmlfactoryofmodelinstancefactory

import com.ligadata.kamanja.metadata.{ ModelDef, BaseElem }
import com.ligadata.KamanjaBase.{ FactoryOfModelInstanceFactory, ModelInstanceFactory, EnvContext, NodeContext }
import com.ligadata.Utils.{ Utils, KamanjaClassLoader, KamanjaLoaderInfo }
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.Exceptions.StackTrace
import com.ligadata.jpmml.JpmmlAdapterFactory

/**
  * An implementation of the FactoryOfModelInstanceFactory trait that supports all of the JPMML models.
  */

object JpmmlFactoryOfModelInstanceFactory extends FactoryOfModelInstanceFactory {
    private[this] val loggerName = this.getClass.getName()
    private[this] val LOG = LogManager.getLogger(loggerName)

    /**
      * As part of the creation of the model instance factory, see to it that there any jars that it needs (either its
      * own or those upon which it depends) are loaded.
      * @param metadataLoader the engine's custom loader to use if/when instances of the model and the dependent jars are to be loaded
      * @param jarPaths  a Set of Paths that contain the dependency jars required by this factory instance to be created
      * @param elem a BaseElem (actually the ModelDef in this case) with an implementation jar and possible dependent jars
      * @return true if all the jar loading was successful.
      */
    private[this] def LoadJarIfNeeded(metadataLoader: KamanjaLoaderInfo, jarPaths: collection.immutable.Set[String], elem: BaseElem): Boolean = {
        val allJars = GetAllJarsFromElem(jarPaths, elem)
        if (allJars.size > 0) {
            return Utils.LoadJars(allJars.toArray, metadataLoader.loadedJars, metadataLoader.loader)
        } else {
            return true
        }
    }

    /**
      * Answer a set of jars that contain the implementation jar and its dependent jars for the supplied BaseElem.
      * The list is checked for valid jar paths before returning.
      * @param jarPaths where jars are located in the cluster node.
      * @param elem the model definition that has a jar implementation and a list of dependency jars
      * @return an Array[String] containing the valid jar paths for the supplied element.
      */
    private[this] def GetAllJarsFromElem(jarPaths: collection.immutable.Set[String], elem: BaseElem): Set[String] = {
        var allJars: Array[String] = null

        val jarname = if (elem.JarName == null) "" else elem.JarName.trim

        if (elem.DependencyJarNames != null && elem.DependencyJarNames.nonEmpty && jarname.nonEmpty) {
            allJars = elem.DependencyJarNames :+ jarname
        } else if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0) {
            allJars = elem.DependencyJarNames
        } else if (jarname.nonEmpty) {
            allJars = Array(jarname)
        } else {
            return Set[String]()
        }

        return allJars.map(j => Utils.GetValidJarFile(jarPaths, j)).toSet
    }


    /**
      * Instantiate a model instance factory for the supplied ''modelDef''.  The model instance factory can instantiate
      * instances of the model described by the ModelDef.
      * @param modelDef the metadatata object for which a model factory instance will be created.
      * @param nodeContext the NodeContext that provides access to model state and kamanja engine services.
      * @param loaderInfo the engine's custom loader to use if/when instances of the model and the dependent jars are to be loaded
      * @param jarPaths a Set of Paths that contain the dependency jars required by this factory instance to be created
      * @return a ModelInstanceFactory or null if bogus information has been supplied.
      *
      * Fixme: Passing the jarPaths as a Set suggests that there is no search order in the path.  Should we have an ordered list
      * instead to allow for alternate implementations where the lib/application directory is first followed by the lib/system
      * directory?  This could be fruitfully used to drop in a fixed implementation ahead of a broken one... this all assumes
      * there is a command to force reload of jars for a given model.
      */
    override def getModelInstanceFactory(modelDef: ModelDef
                                         , nodeContext: NodeContext
                                         , loaderInfo: KamanjaLoaderInfo
                                         , jarPaths: collection.immutable.Set[String]): ModelInstanceFactory = {

        LoadJarIfNeeded(loaderInfo, jarPaths, modelDef)

        val isReasonable : Boolean = (modelDef != null && modelDef.FullNameWithVer != null && modelDef.FullNameWithVer.nonEmpty)
        val mdlInstanceFactory : ModelInstanceFactory = if (isReasonable) {
            val factory: JpmmlAdapterFactory = new JpmmlAdapterFactory(modelDef, nodeContext)

            if (factory == null) {
                LOG.error(s"Failed to instantiate ModelInstanceFactory... name = $modelDef.FullNameWithVer")
            }
            factory
        }  else {
            null
        }

        mdlInstanceFactory
    }

    /**
      * Answer a model definition for the supplied model string, input message, output message and jarPaths.
      *
      * NOTE: Currently not used.
      *
      * @param nodeContext the NodeContext that provides access to model state and kamanja engine services.
      * @param modelString the model source (for those models that supply source)
      * @param inputMessage the namespace.name.version of the input message this model consumes
      * @param outputMessage the namespace.name.version of the output message this model produces (if any)
      * @param loaderInfo the engine's custom loader to use if/when instances of the model and the dependent jars are to be loaded
      * @param jarPaths a Set of Paths that contain the dependency jars required by this factory instance to be created
      * @return a ModelDef instance
      */
    override def prepareModel(nodeContext: NodeContext
                              , modelString: String
                              , inputMessage: String
                              , outputMessage: String
                              , loaderInfo: KamanjaLoaderInfo
                              , jarPaths: collection.immutable.Set[String]): ModelDef = {
        null
    }
}


