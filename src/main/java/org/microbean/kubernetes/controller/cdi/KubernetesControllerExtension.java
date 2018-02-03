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

import java.io.Closeable;

import java.lang.annotation.Annotation;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.CountDownLatch;

import java.util.function.Consumer;

import javax.annotation.Priority;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.Observes;

import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.inject.spi.ObserverMethod;
import javax.enterprise.inject.spi.ProcessManagedBean;
import javax.enterprise.inject.spi.ProcessObserverMethod;
import javax.enterprise.inject.spi.ProcessProducerField;
import javax.enterprise.inject.spi.ProcessProducerMethod;

import javax.enterprise.util.TypeLiteral;

// import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;
import io.fabric8.kubernetes.client.dsl.Operation;

import org.microbean.cdi.AbstractBlockingExtension;
import org.microbean.cdi.Annotations;

import org.microbean.kubernetes.controller.AbstractEventDistributor;
import org.microbean.kubernetes.controller.Controller;
import org.microbean.kubernetes.controller.EventQueue;

import static javax.interceptor.Interceptor.Priority.LIBRARY_AFTER;
import static javax.interceptor.Interceptor.Priority.LIBRARY_BEFORE;

public class KubernetesControllerExtension extends AbstractBlockingExtension {

  private final Collection<Controller<?>> controllers;
  
  private final Map<Set<Annotation>, Bean<?>> eventSelectorBeans;

  private final Set<Bean<?>> beans;
  
  public KubernetesControllerExtension() {
    super();
    this.eventSelectorBeans = new HashMap<>();
    this.beans = new HashSet<>();
    this.controllers = new ArrayList<>();
  }

  public KubernetesControllerExtension(final CountDownLatch latch) {
    super(latch);
    this.eventSelectorBeans = new HashMap<>();
    this.beans = new HashSet<>();
    this.controllers = new ArrayList<>();
  }

  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerMethod(@Observes final ProcessProducerMethod<X, ?> event, final BeanManager beanManager) {
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
  }

  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerField(@Observes final ProcessProducerField<X, ?> event, final BeanManager beanManager) {
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
  }

  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processManagedBean(@Observes final ProcessManagedBean<X> event, final BeanManager beanManager) {
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
  }

  @SuppressWarnings("rawtypes")
  private final void processPotentialEventSelectorBean(final Bean<?> bean, final BeanManager beanManager) {
    if (bean != null) {
      final Type operationType = getOperationType(bean);
      if (operationType != null) {
        final Set<Annotation> kubernetesEventSelectors = Annotations.retainAnnotationsQualifiedWith(bean.getQualifiers(), KubernetesEventSelector.class, beanManager);
        if (kubernetesEventSelectors != null && !kubernetesEventSelectors.isEmpty()) {
          synchronized (this.eventSelectorBeans) {
            this.eventSelectorBeans.put(kubernetesEventSelectors, bean);
          }
        }
      }
    }
  }

  // Observer method processors are guaranteed by the specification to
  // be invoked after ProcessBean events.
  private final void processObserverMethod(@Observes final ProcessObserverMethod<? extends org.microbean.kubernetes.controller.Event<? extends HasMetadata>, ?> event, final BeanManager beanManager) {
    if (event != null) {
      final ObserverMethod<? extends org.microbean.kubernetes.controller.Event<? extends HasMetadata>> observerMethod = event.getObserverMethod();
      if (observerMethod != null) {
        final Set<Annotation> kubernetesEventSelectors = Annotations.retainAnnotationsQualifiedWith(observerMethod.getObservedQualifiers(), KubernetesEventSelector.class, beanManager);
        if (kubernetesEventSelectors != null && !kubernetesEventSelectors.isEmpty()) {
          final Bean<?> bean;
          synchronized (this.eventSelectorBeans) {
            bean = this.eventSelectorBeans.remove(kubernetesEventSelectors);
          }
          synchronized (this.beans) {
            this.beans.add(bean);
          }
        }
      }
    }
  }

  private final void processAfterBeanDiscovery(@Observes final AfterBeanDiscovery event) {
    if (event != null) {
      synchronized (this.eventSelectorBeans) {
        this.eventSelectorBeans.clear();
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private final <T extends HasMetadata, X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> void startController(@Observes @Initialized(ApplicationScoped.class) @Priority(LIBRARY_AFTER) final Object ignored, final BeanManager beanManager) throws InterruptedException {
    if (beanManager != null) {
      
      // Go through all the beans we have
      synchronized (this.beans) {
        for (final Bean<?> bean : this.beans) {
          assert bean != null;
          
          final Type operationType = getOperationType(bean);
          assert operationType != null; // ...or we wouldn't have gotten here
          
          @SuppressWarnings("unchecked") // we know an Operation is of type X
            final X contextualReference = (X)beanManager.getReference(bean, operationType, beanManager.createCreationalContext(bean));
          
          final Map<Object, T> knownObjects = new HashMap<>();
          final Consumer<EventQueue<? extends T>> eventDistributor = new EventDistributor<T>(knownObjects, bean.getQualifiers());
          
          final Controller<T> controller = new Controller<>(contextualReference, knownObjects, eventDistributor);
          controller.start();
          synchronized (this.controllers) {
            this.controllers.add(controller);
          }
        }
      }

    }
  }

  private final void stopControllers(@Observes @BeforeDestroyed(ApplicationScoped.class) @Priority(LIBRARY_BEFORE) final Object ignored) throws Exception {
    Exception exception = null;
    synchronized (this.controllers) {
      for (final Controller<?> controller : this.controllers) {
        assert controller != null;
        try {
          controller.close();
        } catch (final RuntimeException runtimeException) {
          if (exception == null) {
            exception = runtimeException;
          } else {
            exception.addSuppressed(runtimeException);
          }
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  private static final Type getOperationType(final Bean<?> bean) {
    final Type returnValue;
    if (bean == null) {
      returnValue = null;
    } else {
      final Set<Type> beanTypes = bean.getTypes();
      assert beanTypes != null;
      assert !beanTypes.isEmpty();
      Type candidate = null;
      for (final Type beanType : beanTypes) {
        if (beanType instanceof ParameterizedType) {
          final Type rawType = ((ParameterizedType)beanType).getRawType();
          if (rawType instanceof Class && Operation.class.equals((Class<?>)rawType)) {
            candidate = beanType;
            break;
          }
        }
      }
      returnValue = candidate;
    }
    return returnValue;
  }
  
  private static final class EventDistributor<T extends HasMetadata> extends AbstractEventDistributor<T> {

    private static final Annotation[] EMPTY_ANNOTATION_ARRAY = new Annotation[0];
    
    private final Annotation[] qualifiers;
    
    private EventDistributor(final Map<Object, T> knownObjects, final Set<Annotation> qualifiers) {
      super(knownObjects);
      if (qualifiers == null) {
        this.qualifiers = EMPTY_ANNOTATION_ARRAY;
      } else {
        this.qualifiers = qualifiers.toArray(new Annotation[qualifiers.size()]);
      }
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
        cdiEventMachinery.select(eventTypeLiteral, this.qualifiers).fireAsync(controllerEvent);
      }
    }
    
  }
  
}
