/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.config;

import com.hazelcast.jet.impl.deployment.ResourceKind;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Javadoc pending.
 */
public class JobConfig implements Serializable {

    private static final int DEFAULT_RESOURCE_PART_SIZE = 1 << 14;
    private final Set<ResourceConfig> resourceConfigs = new HashSet<>();
    private final Properties properties = new Properties();

    /**
     * @return engine specific properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Javadoc pending
     */
    public int getResourcePartSize() {
        return DEFAULT_RESOURCE_PART_SIZE;
    }

    /**
     * Add class to the job classLoader
     *
     * @param classes classes, which will be used during calculation
     */
    public void addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            resourceConfigs.add(new ResourceConfig(clazz));
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param url location of the JAR file
     */
    public void addJar(URL url) {
        addJar(url, getFileName(url));
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param url location of the JAR file
     * @param id  identifier for the JAR file
     */
    public void addJar(URL url, String id) {
        add(url, id, ResourceKind.JAR);
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param file the JAR file
     */
    public void addJar(File file) {
        try {
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param file the JAR file
     * @param id   identifier for the JAR file
     */
    public void addJar(File file, String id) {
        try {
            addJar(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param path path the JAR file
     */
    public void addJar(String path) {
        try {
            File file = new File(path);
            addJar(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Add JAR to the job classLoader
     *
     * @param path path the JAR file
     * @param id   identifier for the JAR file
     */
    public void addJar(String path, String id) {
        try {
            addJar(new File(path).toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }


    /**
     * Add resource to the job classLoader
     *
     * @param url source url with classes
     */
    public void addResource(URL url) {
        addResource(url, getFileName(url));

    }

    /**
     * Add resource to the job classLoader
     *
     * @param url source url with classes
     * @param id  identifier for the resource
     */
    public void addResource(URL url, String id) {
        add(url, id, ResourceKind.DATA);
    }

    /**
     * Add resource to the job classLoader
     *
     * @param file resource file
     */
    public void addResource(File file) {
        try {
            addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }

    }

    /**
     * Add resource to the job classLoader
     *
     * @param file resource file
     * @param id   identifier for the resource
     */
    public void addResource(File file, String id) {
        try {
            add(file.toURI().toURL(), id, ResourceKind.DATA);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Add resource to the job classLoader
     *
     * @param path path of the resource
     */
    public void addResource(String path) {
        File file = new File(path);
        try {
            addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Add resource to the job classLoader
     *
     * @param path path of the resource
     * @param id   identifier for the resource
     */
    public void addResource(String path, String id) {
        File file = new File(path);
        try {
            addResource(file.toURI().toURL(), id);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Returns all the deployment configurations
     *
     * @return deployment configuration set
     */
    public Set<ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private void add(URL url, String id, ResourceKind type) {
        resourceConfigs.add(new ResourceConfig(url, id, type));
    }

    private String getFileName(URL url) {
        String urlFile = url.getFile();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }

}
