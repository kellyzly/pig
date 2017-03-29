/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.tools.pigstats.spark;


import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparkCounterGroup implements Serializable {
    private String groupName;
    private String groupDisplayName;
    private Map<String, SparkCounter> sparkCounters;

    private transient JavaSparkContext javaSparkContext;

    private SparkCounterGroup() {
        // For serialization.
    }

    public SparkCounterGroup(
            String groupName,
            String groupDisplayName,
            JavaSparkContext javaSparkContext) {
        this.groupName = groupName;
        this.groupDisplayName = groupDisplayName;
        this.javaSparkContext = javaSparkContext;
        this.sparkCounters = new HashMap<String, SparkCounter>();
    }

    public void createCounter(String name, long initValue) {
        SparkCounter counter = new SparkCounter(name, name, groupName, initValue, javaSparkContext);
        sparkCounters.put(name, counter);
    }

    public SparkCounter getCounter(String name) {
        return sparkCounters.get(name);
    }

    public String getGroupName() {
        return groupName;
    }

    public String getGroupDisplayName() {
        return groupDisplayName;
    }

    public void setGroupDisplayName(String groupDisplayName) {
        this.groupDisplayName = groupDisplayName;
    }

    public Map<String, SparkCounter> getSparkCounters() {
        return sparkCounters;
    }
}
