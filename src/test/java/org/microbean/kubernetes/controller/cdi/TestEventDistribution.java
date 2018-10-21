/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2018 microBean.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.microbean.kubernetes.controller.cdi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.Observes;
import javax.enterprise.event.ObservesAsync;

import javax.enterprise.inject.Produces;

import javax.inject.Qualifier;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;

import io.fabric8.kubernetes.client.KubernetesClient;

import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import org.junit.BeforeClass;
import org.junit.Test;

import org.microbean.kubernetes.controller.cdi.annotation.Added;
import org.microbean.kubernetes.controller.cdi.annotation.Modified;
import org.microbean.kubernetes.controller.cdi.annotation.KubernetesEventSelector;
import org.microbean.kubernetes.controller.cdi.annotation.Prior;

import org.microbean.main.Main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@ApplicationScoped
public class TestEventDistribution {

  
  /*
   * Static fields.
   */

  
  /**
   * The number of instances of this class that have been created (in
   * the context of JUnit execution; any other usage is undefined).
   */
  private static int instanceCount;


  /*
   * Constructors.
   */

  public TestEventDistribution() {
    super();
    instanceCount++;
  }

  @Produces
  @ApplicationScoped
  @AllDefaultConfigMapEvents
  private static final NonNamespaceOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> selectAllDefaultConfigMaps(final KubernetesClient client) {
    return client.configMaps().inNamespace("default");
  }

  @Produces
  @ApplicationScoped
  @AllDefaultConfigMapEvents
  private static final Map<Object, ConfigMap> produceCache() {
    return new HashMap<>();
  }

  private final void onConfigMapSynchronizationAddition(@ObservesAsync @AllDefaultConfigMapEvents @Added(synchronization = true) final ConfigMap configMap) {
    assertNotNull(configMap);
  }
  
  private final void onConfigMapModification(@ObservesAsync
                                             @AllDefaultConfigMapEvents
                                             @Modified
                                             final ConfigMap configMap,
                                             @Prior
                                             final Optional<ConfigMap> prior,
                                             @AllDefaultConfigMapEvents
                                             final Map<Object, ConfigMap> cache) {
    assertNotNull(configMap);
    assertNotNull(cache);
    assertNotNull(prior);
    assertTrue("prior is not present", prior.isPresent());
    org.microbean.cdi.AbstractBlockingExtension.unblockAll();
  }

  private final void onStartup(@Observes @Initialized(ApplicationScoped.class) final Object event, final KubernetesControllerExtension extension) throws InterruptedException {
    assertNotNull(extension);
  }
  
  @Test
  public void testContainerStartup() {
    assumeFalse(Boolean.getBoolean("skipClusterTests"));
    System.setProperty("port", "8080");
    final int oldInstanceCount = instanceCount;
    assertEquals(1, oldInstanceCount);
    Main.main(null);
    assertEquals(oldInstanceCount + 1, instanceCount);
  }

  @BeforeClass
  public static void setUpLogging() {
    System.setProperty("java.util.logging.config.file", Thread.currentThread().getContextClassLoader().getResource("logging.properties").getPath());
  }

  @Documented
  @KubernetesEventSelector
  @Qualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD, ElementType.PARAMETER })
  private @interface AllDefaultConfigMapEvents {

  }
  
}
