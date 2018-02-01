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

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.Observes;

import io.fabric8.kubernetes.api.model.HasMetadata;

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


  private final void onStartup(@Observes @Initialized(ApplicationScoped.class) final Object event, final KubernetesControllerExtension extension) throws InterruptedException {
    assertNotNull(extension);
  }

  private final void onEvent(@Observes final org.microbean.kubernetes.controller.Event<? extends HasMetadata> event) {
    System.out.println("*** event: " + event);
    org.microbean.cdi.AbstractBlockingExtension.unblockAll();
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
  
}
