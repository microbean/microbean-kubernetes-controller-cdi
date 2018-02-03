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
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.Observes;
import javax.enterprise.event.ObservesAsync;

import javax.enterprise.inject.Produces;

import javax.inject.Qualifier;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;

import io.fabric8.kubernetes.client.KubernetesClient;

import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Operation;
import io.fabric8.kubernetes.client.dsl.Resource;

import org.junit.Test;

import org.microbean.main.Main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
  @AllConfigMapEvents
  private static final Operation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> selectAllConfigMaps(final KubernetesClient client) {
    return client.configMaps();
  }

  private final void onConfigMapEvent(@ObservesAsync @AllConfigMapEvents final org.microbean.kubernetes.controller.Event<? extends HasMetadata> event) {
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

  @Documented
  @KubernetesEventSelector
  @Qualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD, ElementType.PARAMETER })
  private @interface AllConfigMapEvents {

  }
  
}
