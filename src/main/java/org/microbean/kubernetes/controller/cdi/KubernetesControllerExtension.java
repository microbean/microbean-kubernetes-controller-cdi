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

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.CountDownLatch;

import java.util.function.Consumer;

import javax.annotation.Priority;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.Observes;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;

import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.inject.spi.ProcessObserverMethod;

import javax.enterprise.util.TypeLiteral;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;

import org.microbean.cdi.AbstractBlockingExtension;

import org.microbean.kubernetes.controller.AbstractEventDistributor;
import org.microbean.kubernetes.controller.Controller;
import org.microbean.kubernetes.controller.EventQueue;

import static javax.interceptor.Interceptor.Priority.LIBRARY_AFTER;
import static javax.interceptor.Interceptor.Priority.LIBRARY_BEFORE;

public class KubernetesControllerExtension extends AbstractBlockingExtension {

  private volatile Controller<?> controller;
  
  public KubernetesControllerExtension() {
    super();
  }

  public KubernetesControllerExtension(final CountDownLatch latch) {
    super(latch);
  }

  private final void startController(@Observes @Initialized(ApplicationScoped.class) @Priority(LIBRARY_AFTER) final Object ignored, final BeanManager beanManager) throws InterruptedException {
    if (beanManager != null) {
      final Instance<Object> instance = beanManager.createInstance();
      assert instance != null;
      final Instance<KubernetesClient> kubernetesClientInstance = instance.select(KubernetesClient.class);
      assert kubernetesClientInstance != null;
      final KubernetesClient kubernetesClient;
      if (kubernetesClientInstance.isUnsatisfied() || kubernetesClientInstance.isAmbiguous()) {
        kubernetesClient = null;
      } else {
        assert kubernetesClientInstance.isResolvable();
        kubernetesClient = kubernetesClientInstance.get();
        assert kubernetesClient != null;
      }
      if (kubernetesClient != null) {

        final Map<Object, ConfigMap> knownObjects = new HashMap<>();
        final Consumer<EventQueue<? extends ConfigMap>> eventDistributor = new EventDistributor<ConfigMap>(knownObjects);
        
        // TODO: optimize for ...if we detect only one kind of
        // observer method, i.e. one interested in Pods, and that's
        // the only such controller-related observer method, then we
        // could take a different path here.
        final Controller<ConfigMap> controller = new Controller<>(kubernetesClient.configMaps(), knownObjects, eventDistributor);
        this.controller = controller;
        this.controller.start();
      }
    }
  }

  private final void stopController(@Observes @BeforeDestroyed(ApplicationScoped.class) @Priority(LIBRARY_BEFORE) final Object ignored) throws Exception {
    final Controller<?> controller = this.controller;
    if (controller != null) {
      controller.close();
    }
  }

  private static final class EventDistributor<T extends HasMetadata> extends AbstractEventDistributor<T> {

    private EventDistributor(final Map<Object, T> knownObjects) {
      super(knownObjects);
    }

    @Override
    protected final void fireEvent(final org.microbean.kubernetes.controller.Event<T> controllerEvent) {
      if (controllerEvent != null) {
        // TODO: I'm sure there's a more clean way to do all this.
        final BeanManager beanManager = CDI.current().getBeanManager();
        assert beanManager != null;
        final javax.enterprise.event.Event<Object> cdiEventMachinery = beanManager.getEvent();
        assert cdiEventMachinery != null;
        final TypeLiteral<org.microbean.kubernetes.controller.Event<? extends T>> eventTypeLiteral = new TypeLiteral<org.microbean.kubernetes.controller.Event<? extends T>>() {
            private static final long serialVersionUID = 1L;
          };
        System.out.println("*** firing: " + controllerEvent);
        cdiEventMachinery.select(eventTypeLiteral).fireAsync(controllerEvent);
      }
    }
    
  }
  
}
