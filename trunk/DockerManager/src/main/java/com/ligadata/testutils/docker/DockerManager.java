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

package com.ligadata.testutils.docker;

import com.github.dockerjava.api.*;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.*;
import com.github.dockerjava.core.command.PullImageResultCallback;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by will on 2/24/16.
 */
public class DockerManager {
    private DockerClient dockerClient;
    private String dockerHost;

    public DockerManager() {
        try {
            DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder().build();
            dockerClient = DockerClientBuilder.getInstance(config).build();
            URI uri = config.getUri();
            dockerHost = uri.getHost();
        }
        catch(Exception e) {
            throw new RuntimeException("Failed to instantiate DockerManager", e);
        }
    }

    public Info getInfo() {
        System.out.println("Retrieving Docker Info");
        Info info = dockerClient.infoCmd().exec();
        return info;
    }

    public List<SearchItem> searchDockerImages(String term) {
        return dockerClient.searchImagesCmd(term).exec();
    }

    public String startContainer(String imageName, Map<Integer, Integer> exposedPorts, Map<String, String> envVariables) throws InterruptedException {
        Ports portBindings = new Ports();
        List<ExposedPort> ePorts = new ArrayList<>();
        List<String> envVars = new ArrayList<>();

        for(Integer port : exposedPorts.keySet()) {
            ePorts.add(new ExposedPort(port));
            portBindings.bind(new ExposedPort(port), Ports.Binding(exposedPorts.get(port)));
        }

        for(String envVar: envVariables.keySet()) {
            envVars.add(envVar + "=" + envVariables.get(envVar));
        }

        System.out.println("Pulling image: " + imageName + "... This may take a few minutes");
        dockerClient.pullImageCmd(imageName).exec(new PullImageResultCallback()).awaitCompletion();

        CreateContainerResponse container = dockerClient.createContainerCmd(imageName)
                .withPortBindings(portBindings)
                .withExposedPorts(ePorts.toArray(new ExposedPort[ePorts.size()]))
                .withEnv(envVars.toArray(new String[envVars.size()]))
                .exec();

        System.out.println("Starting Docker Container with Image Name: " + imageName + " and with ID: " + container.getId());

        dockerClient.startContainerCmd(container.getId()).exec();
        return container.getId();
    }

    public String startContainer(String imageName) throws InterruptedException {
        return startContainer(imageName, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    public String startContainer(String imageName, Map<Integer, Integer> portMap) throws InterruptedException {
        return startContainer(imageName, portMap, Collections.EMPTY_MAP);
    }

    public void stopContainer(String containerId) {
        System.out.println("Stopping Docker Container with container ID: " + containerId);
        dockerClient.stopContainerCmd(containerId).exec();
        dockerClient.waitContainerCmd(containerId).exec();
    }

    public String getDockerHost() {
        return dockerHost;
    }
}
