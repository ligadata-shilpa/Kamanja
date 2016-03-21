package com.ligadata.KamanjaBase;

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

public interface SerializeDeserialize {
    /**
     * Serialize the supplied object, v.
     * @param v some object to be serialized
     * @return a byte array containing the serialized representation of v
     */
    public byte[] serialize(ContainerInterface v);
    /**
     * From the supplied byte array, resurrect an instance of type T.
     * @param b the byte array containing the serialized instance of type T
     * @return an instance of T
     */
    public ContainerInterface deserialize(byte[] b);

    /**
     * Allow the object resolver to be changed by a client object if desired.
     * @param objRes
     */
    public void setObjectResolver(ObjectResolver objRes);
}