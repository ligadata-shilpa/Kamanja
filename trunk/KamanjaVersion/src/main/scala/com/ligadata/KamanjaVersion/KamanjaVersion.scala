/*
 * Copyright 2016 ligaDATA
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

package com.ligadata.KamanjaVersion

object KamanjaVersion {
  val majorVersion = 1
  val minorVersion = 3
  val microVersion = 3
  val buildNumber = 0

  def getMajorVersion:int = majorVersion
  def getMinorVersion:int = minorVersion
  def getMicroVersion:int = microVersion
  def getBuildNumber:int = buildNumber

  def getVersions: (int, int, int) = ((majorVersion, minorVersion, microVersion))

  def getVersionsWithBuildNumber: (int, int, int, int) = ((majorVersion, minorVersion, microVersion, buildNumber))

  def getVersionString: String = ("Kamanja version " + majorVersion + "." + minorVersion + "." + microVersion)

  def getVersionStringWithBuildNumber: String = ("Kamanja version " + majorVersion + "." + minorVersion + "." + microVersion + "." + buildNumber)

  def print: String = println(getVersionStringWithBuildNumber)

}

