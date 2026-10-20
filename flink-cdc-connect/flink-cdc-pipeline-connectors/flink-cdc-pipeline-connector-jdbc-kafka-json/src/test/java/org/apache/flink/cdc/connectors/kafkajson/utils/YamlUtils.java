/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafkajson.utils;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Map;

/** Yaml loading utility. */
public final class YamlUtils {

    private static final ThreadLocal<Yaml> YAML_THREAD_LOCAL = ThreadLocal.withInitial(Yaml::new);

    private YamlUtils() {}

    public static <T> T loadYamlFromFile(String filePath, Class<T> classType)
            throws IOException, YAMLException {
        try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
            return YAML_THREAD_LOCAL.get().loadAs(fileInputStream, classType);
        } catch (IOException e) {
            throw new IOException("Failed to read file: " + filePath, e);
        } catch (YAMLException e) {
            throw new YAMLException("Failed to parse yaml: " + filePath, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadYamlFromFile(String filePath)
            throws IOException, YAMLException {
        return loadYamlFromFile(filePath, (Class<Map<String, Object>>) (Class<?>) Map.class);
    }

    public static <T> T loadYamlFromResource(String resourcePath, Class<T> classType)
            throws IOException, YAMLException {
        try (InputStream inputStream =
                YamlUtils.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return YAML_THREAD_LOCAL.get().loadAs(inputStream, classType);
        } catch (IOException e) {
            throw new IOException("Failed to read resource: " + resourcePath, e);
        } catch (YAMLException e) {
            throw new YAMLException("Failed to parse yaml: " + resourcePath, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadYamlFromResource(String resourcePath)
            throws IOException, YAMLException {
        return loadYamlFromResource(
                resourcePath, (Class<Map<String, Object>>) (Class<?>) Map.class);
    }

    public static <T> T loadYamlFromString(String yamlString, Class<T> classType)
            throws YAMLException {
        try (StringReader stringReader = new StringReader(yamlString)) {
            return YAML_THREAD_LOCAL.get().loadAs(stringReader, classType);
        } catch (YAMLException e) {
            throw new YAMLException("Failed to parse yaml", e);
        }
    }
}
