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
import java.io.IOException;

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

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Priority;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;

import javax.enterprise.event.NotificationOptions;
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
import javax.enterprise.inject.spi.ProcessSyntheticBean;
import javax.enterprise.inject.spi.ProcessSyntheticObserverMethod;

import javax.enterprise.util.TypeLiteral;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;
import io.fabric8.kubernetes.client.dsl.Operation;

import org.microbean.cdi.AbstractBlockingExtension;
import org.microbean.cdi.Annotations;

import org.microbean.kubernetes.controller.AbstractEvent;
import org.microbean.kubernetes.controller.Controller;
import org.microbean.kubernetes.controller.EventQueue;
import org.microbean.kubernetes.controller.EventDistributor;

import static javax.interceptor.Interceptor.Priority.LIBRARY_AFTER;
import static javax.interceptor.Interceptor.Priority.LIBRARY_BEFORE;

/**
 * An {@link AbstractBlockingExtension} that distributes Kubernetes
 * events to interested listeners asynchronously.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see AbstractBlockingExtension
 *
 * @see Controller
 */
public class KubernetesControllerExtension extends AbstractBlockingExtension {


  /*
   * Instance fields.
   */

  
  private final Collection<Controller<?>> controllers;
  
  private final Map<Set<Annotation>, Bean<?>> eventSelectorBeans;

  private final Set<Bean<?>> beans;

  /**
   * A {@link Logger} for use by this {@link KubernetesControllerExtension}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #createLogger()
   */
  private final Logger logger;


  /*
   * Constructors.
   */

  
  /**
   * Creates a new {@link KubernetesControllerExtension}.
   */
  public KubernetesControllerExtension() {
    this(new CountDownLatch(1));
  }

  /**
   * Creates a new {@link KubernetesControllerExtension}.
   *
   * <p>Most users should prefer the {@linkplain
   * #KubernetesControllerExtension() zero-argument constructor}
   * instead.</p>
   *
   * @param latch a {@link CountDownLatch} passed to the {@link
   * AbstractBlockingExtension#AbstractBlockingExtension(CountDownLatch)}
   * constructor; must not be {@code null}
   *
   * @see
   * AbstractBlockingExtension#AbstractBlockingExtension(CountDownLatch)
   */
  protected KubernetesControllerExtension(final CountDownLatch latch) {
    super(latch);
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    this.eventSelectorBeans = new HashMap<>();
    this.beans = new HashSet<>();
    this.controllers = new ArrayList<>();
  }


  /*
   * Instance methods.
   */
  

  /**
   * Returns a {@link Logger} for use by this {@link
   * KubernetesControllerExtension}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @return a non-{@code null} {@link Logger}
   */
  protected Logger createLogger() {
    return Logger.getLogger(this.getClass().getName());
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessProducerMethod} event and calls the {@link
   * #processPotentialEventSelectorBean(Bean, BeanManager)} method
   * with the return value of the event's {@link
   * ProcessProducerMethod#getBean()} method and the supplied {@link
   * BeanManager}.
   *
   * @param <X> a type that is both {@link Listable} and {@link
   * VersionWatchable}
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorBean(Bean, BeanManager)
   */
  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerMethod(@Observes final ProcessProducerMethod<X, ?> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processProducerMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessProducerField} event and calls the {@link
   * #processPotentialEventSelectorBean(Bean, BeanManager)} method
   * with the return value of the event's {@link
   * ProcessProducerField#getBean()} method and the supplied {@link
   * BeanManager}.
   *
   * @param <X> a type that is both {@link Listable} and {@link
   * VersionWatchable}
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorBean(Bean, BeanManager)
   */
  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerField(@Observes final ProcessProducerField<X, ?> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processProducerField";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessManagedBean} event and calls the {@link
   * #processPotentialEventSelectorBean(Bean, BeanManager)} method
   * with the return value of the event's {@link
   * ProcessManagedBean#getBean()} method and the supplied {@link
   * BeanManager}.
   *
   * @param <X> a type that is both {@link Listable} and {@link
   * VersionWatchable}
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorBean(Bean, BeanManager)
   */
  // Ideally, we could do this all in a ProcessBean observer method.
  // See https://issues.jboss.org/browse/WELD-2461.
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processManagedBean(@Observes final ProcessManagedBean<X> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processManagedBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessSyntheticBean} event and calls the {@link
   * #processPotentialEventSelectorBean(Bean, BeanManager)} method
   * with the return value of the event's {@link
   * ProcessSyntheticBean#getBean()} method and the supplied {@link
   * BeanManager}.
   *
   * @param <X> a type that is both {@link Listable} and {@link
   * VersionWatchable}
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorBean(Bean, BeanManager)
   */
  @SuppressWarnings("rawtypes")
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processSyntheticBean(@Observes final ProcessSyntheticBean<X> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processSyntheticBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorBean(event.getBean(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessObserverMethod} event and calls the {@link
   * #processPotentialEventSelectorObserverMethod(ObserverMethod,
   * BeanManager)} method with the return value of the event's {@link
   * ProcessObserverMethod#getObserverMethod()} method and the
   * supplied {@link BeanManager}.
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorObserverMethod(ObserverMethod,
   * BeanManager)
   */
  // Observer method processors are guaranteed by the specification to
  // be invoked after ProcessBean events.
  private final void processObserverMethod(@Observes final ProcessObserverMethod<? extends org.microbean.kubernetes.controller.Event<? extends HasMetadata>, ?> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorObserverMethod(event.getObserverMethod(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessSyntheticObserverMethod} event and calls the {@link
   * #processPotentialEventSelectorObserverMethod(ObserverMethod,
   * BeanManager)} method with the return value of the event's {@link
   * ProcessSyntheticObserverMethod#getObserverMethod()} method and
   * the supplied {@link BeanManager}.
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see #processPotentialEventSelectorObserverMethod(ObserverMethod,
   * BeanManager)
   */
  // Observer method processors are guaranteed by the specification to
  // be invoked after ProcessBean events.
  private final void processSyntheticObserverMethod(@Observes final ProcessSyntheticObserverMethod<? extends org.microbean.kubernetes.controller.Event<? extends HasMetadata>, ?> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processSyntheticObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    if (event != null) {
      this.processPotentialEventSelectorObserverMethod(event.getObserverMethod(), beanManager);
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * AfterBeanDiscovery} event and, since all bean discovery is done,
   * clears out the contents of the {@link #eventSelectorBeans} field.
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @see #eventSelectorBeans
   */
  private final void processAfterBeanDiscovery(@Observes final AfterBeanDiscovery event) {
    final String cn = this.getClass().getName();
    final String mn = "processAfterBeanDiscovery";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, event);
    }
    if (event != null) {
      synchronized (this.eventSelectorBeans) {
        this.eventSelectorBeans.clear();
      }
      // TODO: consider: we have the ability to create Controller
      // beans here out of other bean raw materials
      // (e.g. appropriately-qualified knownObjects etc.).
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the {@linkplain Initialized
   * initialization} of the {@linkplain ApplicationScoped application
   * scope} at {@link
   * javax.interceptor.Interceptor.Priority#LIBRARY_AFTER
   * LIBRARY_AFTER} {@linkplain Priority priority} and, now that this
   * extension is in a position to know all event observers that might
   * be interested in Kubernetes events, arranges to funnel such
   * events through a resilient pipeline into their hands
   * asynchronously.
   *
   * @param <T> a type that extends {@link HasMetadata}; e.g. a
   * Kubernetes resource type
   *
   * @param <X> a type that is both a {@link Listable} and a {@link
   * VersionWatchable}
   *
   * @param ignored the event itself; ignored by this implementation;
   * may be {@code null}
   *
   * @param beanManager the {@link BeanManager} in effect for this CDI
   * container; may be {@code null} in which case no action will be
   * taken
   *
   * @see #stopControllers(Object)
   */
  @SuppressWarnings("rawtypes")
  private final <T extends HasMetadata, X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> void startControllers(@Observes @Initialized(ApplicationScoped.class) @Priority(LIBRARY_AFTER) final Object ignored, final BeanManager beanManager) { 
    final String cn = this.getClass().getName();
    final String mn = "startControllers";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { ignored, beanManager });
    }
    if (beanManager != null) {
      
      // Go through all the beans we have
      synchronized (this.beans) {
        for (final Bean<?> bean : this.beans) {
          assert bean != null;
          
          final Type operationType = getOperationType(bean);
          assert operationType != null;
          
          @SuppressWarnings("unchecked") // we know an Operation is of type X
          final X contextualReference = (X)beanManager.getReference(bean, operationType, beanManager.createCreationalContext(bean));
          
          final Map<Object, T> knownObjects = new HashMap<>();

          final Set<Annotation> qualifiers = bean.getQualifiers();

          final Set<Bean<?>> notificationOptionsBeans = beanManager.getBeans(NotificationOptions.class, qualifiers.toArray(new Annotation[qualifiers.size()]));

          final Bean<?> notificationOptionsBean = beanManager.resolve(notificationOptionsBeans);

          final NotificationOptions notificationOptions;
          if (notificationOptionsBean == null) {
            notificationOptions = null;
          } else {
            notificationOptions = (NotificationOptions)beanManager.getReference(notificationOptionsBean, NotificationOptions.class, beanManager.createCreationalContext(notificationOptionsBean));
          }
          
          final Consumer<AbstractEvent<? extends T>> cdiEventDistributor = new CDIEventDistributor<T>(qualifiers, notificationOptions);

          final EventDistributor<T> eventDistributor = new EventDistributor<>(knownObjects);
          eventDistributor.addConsumer(cdiEventDistributor);
          
          final Controller<T> controller = new Controller<>(contextualReference, knownObjects, eventDistributor);
          controller.start();
          synchronized (this.controllers) {
            this.controllers.add(controller);
          }
        }
      }

    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the {@linkplain BeforeDestroyed
   * imminent destruction} of the {@linkplain ApplicationScoped
   * application scope} at {@link
   * javax.interceptor.Interceptor.Priority#LIBRARY_BEFORE
   * LIBRARY_BEFORE} {@linkplain Priority priority} and stops any
   * {@linkplain #controllers <tt>Controller</tt> instances that were
   * started} by {@linkplain Controller#close() closing} them.
   *
   * @param ignored the actual {@link BeforeDestroyed} event; ignored;
   * may be {@code null}
   *
   * @exception IOException if {@link Controller#close()} throws an
   * {@link IOException}
   *
   * @see #startControllers(Object, BeanManager)
   */
  private final void stopControllers(@Observes @BeforeDestroyed(ApplicationScoped.class) @Priority(LIBRARY_BEFORE) final Object ignored) throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "stopControllers";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, ignored);
    }

    Exception exception = null;
    synchronized (this.controllers) {
      for (final Controller<?> controller : this.controllers) {
        assert controller != null;
        try {
          controller.close();
        } catch (final IOException | RuntimeException closeException) {
          if (closeException == null) {
            exception = closeException;
          } else {
            exception.addSuppressed(closeException);
          }
        }
      }
    }

    // TODO: go through all the CDI event broadcasters and close them
    // too; we don't currently keep track of them

    
    if (exception instanceof IOException) {
      throw (IOException)exception;
    } else if (exception instanceof RuntimeException) {
      throw (RuntimeException)exception;
    } else if (exception != null) {
      throw new IllegalStateException(exception.getMessage(), exception);
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Non-observer methods.
   */


  /**
   * Given a {@link Bean}, checks to see if it is annotated with at
   * least one annotation that is, in turn, annotated with {@link
   * KubernetesEventSelector}, and, if so, adds it to a list of
   * candidate sources of objects that are both {@link Listable} and
   * {@link VersionWatchable}.
   *
   * @param bean the {@link Bean} to inspect; may be {@code null} in
   * which case no action will be taken
   *
   * @param beanManager the {@link BeanManager} in effect for the
   * current CDI container; may be {@code null}
   *
   * @see Annotations#retainAnnotationsQualifiedWith(Collection,
   * Class, BeanManager)
   *
   * @see KubernetesEventSelector
   */
  @SuppressWarnings("rawtypes")
  private final void processPotentialEventSelectorBean(final Bean<?> bean, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processPotentialEventSelectorBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { bean, beanManager });
    }
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
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Given an {@link ObserverMethod}, checks to see if its event
   * parameter is annotated with at least one annotation that is, in
   * turn, annotated with {@link KubernetesEventSelector}, and, if so,
   * makes sure that any {@link Bean}s whose {@linkplain
   * Bean#getQualifiers() qualifiers} line up are retained by this
   * extension as sources of {@link Listable} and {@link
   * VersionWatchable} instances.
   *
   * @param observerMethod the {@link ObserverMethod} to inspect; may
   * be {@code null} in which case no action will be taken
   *
   * @param beanManager the {@link BeanManager} in effect for the
   * current CDI container; may be {@code null}
   *
   * @see Annotations#retainAnnotationsQualifiedWith(Collection,
   * Class, BeanManager)
   *
   * @see KubernetesEventSelector
   */
  private final void processPotentialEventSelectorObserverMethod(final ObserverMethod<? extends org.microbean.kubernetes.controller.Event<? extends HasMetadata>> observerMethod, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processPotentialEventSelectorObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, observerMethod);
    }
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
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, observerMethod);
    }
  }
  

  /*
   * Static methods.
   */


  /**
   * A bit of a hack to return the {@link Type} equivalent of {@link
   * Operation Operation.class} from the supplied {@link Bean}'s
   * {@linkplain Bean#getTypes() types}, if it is present among them,
   * and to return {@code null} otherwise.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>{@link Operation Operation.class} is the most general
   * interface that implements both {@link Listable} and {@link
   * VersionWatchable}, which is a common constraint for {@link
   * Controller} operations.</p>
   *
   * @param bean the {@link Bean} to inspect; may be {@code null} in
   * which case {@code null} will be returned
   *
   * @return a {@link Type} equal to {@link Operation
   * Operation.class}, or {@code null}
   *
   * @see Operation
   */
  private static final Type getOperationType(final Bean<?> bean) {
    final String cn = KubernetesControllerExtension.class.getName();
    final Logger logger = Logger.getLogger(cn);
    assert logger != null;
    final String mn = "getOperationType";
    if (logger.isLoggable(Level.FINER)) {
      logger.entering(cn, mn, bean);
    }
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
    if (logger.isLoggable(Level.FINER)) {
      logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }


  /*
   * Inner and nested classes.
   */

  
  private static final class CDIEventDistributor<T extends HasMetadata> implements Consumer<AbstractEvent<? extends T>> {

    private static final Annotation[] EMPTY_ANNOTATION_ARRAY = new Annotation[0];
    
    private final Annotation[] qualifiers;

    private final NotificationOptions notificationOptions;

    private final Logger logger;
    
    private CDIEventDistributor(final Set<Annotation> qualifiers, final NotificationOptions notificationOptions) {
      super();
      final String cn = this.getClass().getName();      
      this.logger = Logger.getLogger(cn);
      assert this.logger != null;
      final String mn = "<init>";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, new Object[] { qualifiers, notificationOptions });
      }
      if (qualifiers == null) {
        this.qualifiers = EMPTY_ANNOTATION_ARRAY;
      } else {
        this.qualifiers = qualifiers.toArray(new Annotation[qualifiers.size()]);
      }
      this.notificationOptions = notificationOptions;
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }

    @Override
    public final void accept(final AbstractEvent<? extends T> controllerEvent) {
      final String cn = this.getClass().getName();
      final String mn = "accept";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, controllerEvent);
      }
      if (controllerEvent != null) {
        // TODO: I'm sure there's a more clean way to do all this.
        final BeanManager beanManager = CDI.current().getBeanManager();
        assert beanManager != null;
        final javax.enterprise.event.Event<Object> cdiEventMachinery = beanManager.getEvent();
        assert cdiEventMachinery != null;
        final TypeLiteral<AbstractEvent<? extends T>> eventTypeLiteral = new TypeLiteral<AbstractEvent<? extends T>>() {
            private static final long serialVersionUID = 1L;
          };
        final javax.enterprise.event.Event<AbstractEvent<? extends T>> broadcaster = cdiEventMachinery.select(eventTypeLiteral, this.qualifiers);
        assert broadcaster != null;
        // TODO: we may actually want to do a straight fire here
        // instead of fireAsync, since we're already off the main
        // container thread
        if (this.notificationOptions == null) {
          broadcaster.fireAsync(controllerEvent);
        } else {
          broadcaster.fireAsync(controllerEvent, this.notificationOptions);
        }
      }
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }
    
  }
  
}
