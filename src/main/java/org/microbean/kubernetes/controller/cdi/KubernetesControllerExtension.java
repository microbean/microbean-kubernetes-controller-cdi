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
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import java.util.function.Consumer;
import java.util.function.Function;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Priority;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.ContextNotActiveException;
import javax.enterprise.context.Initialized;

import javax.enterprise.context.spi.AlterableContext;
import javax.enterprise.context.spi.Contextual;
import javax.enterprise.context.spi.CreationalContext;

import javax.enterprise.event.NotificationOptions;
import javax.enterprise.event.Observes;

import javax.enterprise.inject.Default; // for javadoc only

import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanAttributes;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.inject.spi.DeploymentException;
import javax.enterprise.inject.spi.EventContext;
import javax.enterprise.inject.spi.ObserverMethod;
import javax.enterprise.inject.spi.ProcessBean;
import javax.enterprise.inject.spi.ProcessManagedBean;
import javax.enterprise.inject.spi.ProcessObserverMethod;
import javax.enterprise.inject.spi.ProcessProducerField;
import javax.enterprise.inject.spi.ProcessProducerMethod;
import javax.enterprise.inject.spi.ProcessSyntheticBean;
import javax.enterprise.inject.spi.ProcessSyntheticObserverMethod;

import javax.enterprise.inject.spi.configurator.ObserverMethodConfigurator.EventConsumer;

import javax.inject.Qualifier; // for javadoc only
import javax.inject.Scope;

import io.fabric8.kubernetes.api.model.ConfigMap; // for javadoc only
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.KubernetesClient; // for javadoc only
import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;
import io.fabric8.kubernetes.client.dsl.Operation; // for javadoc only

import org.microbean.cdi.AbstractBlockingExtension;
import org.microbean.cdi.Annotations;

import org.microbean.configuration.api.Configurations;

import org.microbean.development.annotation.Issue;

import org.microbean.kubernetes.controller.AbstractEvent;
import org.microbean.kubernetes.controller.Controller;
import org.microbean.kubernetes.controller.EventDistributor;
import org.microbean.kubernetes.controller.SynchronizationEvent;

import org.microbean.kubernetes.controller.cdi.annotation.Added;
import org.microbean.kubernetes.controller.cdi.annotation.Deleted;
import org.microbean.kubernetes.controller.cdi.annotation.Modified;
import org.microbean.kubernetes.controller.cdi.annotation.KubernetesEventSelector;
import org.microbean.kubernetes.controller.cdi.annotation.Prior;

import static javax.interceptor.Interceptor.Priority.LIBRARY_AFTER;
import static javax.interceptor.Interceptor.Priority.LIBRARY_BEFORE;

/**
 * An {@link AbstractBlockingExtension} that distributes Kubernetes
 * events to interested listeners asynchronously.
 *
 * <h1>Usage</h1>
 * 
 * <p>To use this extension, simply place it on your classpath (along
 * with your selection from a menu of certain required runtime
 * dependencies described below).  If you have a mechanism for
 * describing the kinds of Kubernetes resources you'd like to watch
 * for events, and if you have observer methods created to do
 * something with those events, both of which are described below,
 * then this extension will take care of connecting to the Kubernetes
 * API server and listing and watching for new events for you.</p>
 *
 * <h2>Dependency Choices</h2>
 *
 * <p>This extension relies on the presence of certain CDI beans.  In
 * some cases, those beans are not produced by this extension.  For
 * maximum flexibility, this project does not mandate how certain
 * beans are produced.  Below is a list of the beans that are
 * required, and suggested&mdash;but not required&mdash;ways of
 * producing them.</p>
 *
 * <h2>Maven</h2>
 *
 * <p>If you are using Maven, you may indicate that you want this
 * extension to be included on your project's runtime classpath with
 * the following dependency stanza:</p>
 *
 * <blockquote><pre>&lt;dependency&gt;
 *  &lt;groupId&gt;org.microbean&lt;/groupId&gt;
 *  &lt;artifactId&gt;microbean-kubernetes-controller-cdi&lt;/artifactId&gt;
 *  &lt;version&gt;0.2.1&lt;/version&gt;
 *  &lt;scope&gt;runtime&lt;/scope&gt;
 *&lt;/dependency&gt;</pre></blockquote>
 *
 * <h3>{@link KubernetesClient} Bean</h3>
 *
 * <p>This extension indirectly requires that a {@link
 * KubernetesClient} be available in the CDI container (qualified with
 * {@link Default @Default}).  You can use the <a
 * href="https://microbean.github.io/microbean-kubernetes-client-cdi/">microBean
 * Kubernetes Client CDI</a> project for this, or you can arrange to
 * fulfil this requirement yourself.</p>
 *
 * <p>If you are going to use the <a
 * href="https://microbean.github.io/microbean-kubernetes-client-cdi/">microBean
 * Kubernetes Client CDI</a> project to provide a {@link
 * Default}-qualified {@link KubernetesClient}, you can indicate that
 * you want it to be included on your project's runtime classpath with
 * the following dependency stanza:</p>
 *
 * <blockquote><pre>&lt;dependency&gt;
 *  &lt;groupId&gt;org.microbean&lt;/groupId&gt;
 *  &lt;artifactId&gt;microbean-kubernetes-client-cdi&lt;/artifactId&gt;
 *  &lt;version&gt;0.3.1&lt;/version&gt;
 *  &lt;scope&gt;runtime&lt;/scope&gt;
 *&lt;/dependency&gt;</pre></blockquote>
 *
 * <h3>Configuration Beans</h3>
 * 
 * <p>You'll need an implementation of the <a
 * href="https://microbean.github.io/microbean-configuration-api/">microBean
 * Configuration API</a>.  Usually, the <a
 * href="https://microbean.github.io/microbean-configuration/">microBean
 * Configuration</a> project is what you want.  You can indicate that
 * you want it to be included on your project's runtime classpath with
 * the following dependency stanza:</p>
 *
 * <blockquote><pre>&lt;dependency&gt;
 *  &lt;groupId&gt;org.microbean&lt;/groupId&gt;
 *  &lt;artifactId&gt;microbean-configuration&lt;/artifactId&gt;
 *  &lt;version&gt;0.4.2&lt;/version&gt;
 *  &lt;scope&gt;runtime&lt;/scope&gt;
 *&lt;/dependency&gt;</pre></blockquote>
 *
 * <p>You'll need a means of getting that configuration implementation
 * into CDI.  Usually, you would use the <a
 * href="https://microbean.github.io/microbean-configuration-cdi/">microBean
 * Configuration CDI</a> project.  You can indicate that you want it
 * to be included on your project's runtime classpath with the
 * following dependency stanza:</p>
 *
 * <blockquote><pre>&lt;dependency&gt;
 *  &lt;groupId&gt;org.microbean&lt;/groupId&gt;
 *  &lt;artifactId&gt;microbean-configuration-cdi&lt;/artifactId&gt;
 *  &lt;version&gt;0.4.2&lt;/version&gt;
 *  &lt;scope&gt;runtime&lt;/scope&gt;
 *&lt;/dependency&gt;</pre></blockquote>
 *
 * <h2>Event Selectors</h2>
 * 
 * <p>To describe the kinds of Kubernetes resources you're interested
 * in, you'll need one or more <em>event selectors</em> in your CDI
 * application.  An event selector, for the purposes of this class, is
 * a CDI bean (either a managed bean or a producer method, most
 * commonly) with certain types in its {@linkplain Bean#getTypes() set
 * of bean types}.  Specifically, the event selector type, {@code X},
 * must conform to this specification:</p>
 *
 * <blockquote>{@code <X extends Listable<? extends
 * KubernetesResourceList> & VersionWatchable<? extends Closeable,
 * Watcher<? extends HasMetadata>>>}</blockquote>
 *
 * <p>Many return types of methods belonging to {@link
 * KubernetesClient} conveniently conform to this specification.</p>
 *
 * <p>The event selector will also need to be annotated with an
 * annotation that you write describing the sort of event selection it
 * is.  This annotation does not need any elements, but must itself be
 * annotated with the {@link
 * KubernetesEventSelector @KubernetesEventSelector} annotation.  It
 * must be applicable to {@linkplain ElementType#PARAMETER parameters}
 * and your event selector beans, so if as is most common you are
 * writing a producer method it must be applicable to {@linkplain
 * ElementType#METHOD methods} as well.</p>
 *
 * <p>Here is an example producer method that will cause this
 * extension to look for all ConfigMap events:</p>
 *
 * <blockquote><pre>&#64;Produces
 *&#64;{@link ApplicationScoped}
 *&#64;AllConfigMapEvents // see declaration below
 *private static final {@link Operation}&lt;{@link ConfigMap}, ConfigMapList, DoneableConfigMap, Resource&lt;ConfigMap, DoneableConfigMap&gt;&gt; selectAllConfigMaps(final {@link KubernetesClient} client) {
 *  return {@link KubernetesClient#configMaps() client.configMaps()};
 *}}</pre></blockquote>
 *
 * <p>Note in particular that {@link Operation} implements both {@link
 * Listable} and {@link VersionWatchable} with the proper type
 * parameters.</p>
 *
 * <p>The {@code @AllConfigMapEvents} annotation is simply:</p>
 *
 * <blockquote><pre>&#64;Documented
 *&#64;{@link KubernetesEventSelector}
 *&#64;{@link Qualifier}
 *&#64;Retention(value = RetentionPolicy.RUNTIME)
 *&#64;Target({ ElementType.METHOD, ElementType.PARAMETER })
 *public &#64;interface AllConfigMapEvents {
 *
 *}</pre></blockquote>
 *
 * <h2>Observer Methods</h2>
 *
 * <p>Observer methods are where your CDI application actually takes
 * delivery of a Kubernetes resource as a CDI event.</p>
 *
 * <p>You will need a notional pair consisting of an event selector
 * and an observer method that conforms to certain requirements that
 * help "link" it to its associated event selector.  To realize this
 * pair, you write a normal CDI observer method that adheres to the
 * following additional requirements:</p>
 *
 * <ol>
 *
 * <li>Its <em>observed event type</em> is a concrete class that
 * extends {@link HasMetadata} and is the same type with which the
 * associated event selector is primarily concerned.  For example, if
 * your event selector is primarily concerned with {@link ConfigMap}s,
 * then your observer method's observed event type should also be
 * {@link ConfigMap}.</li>
 *
 * <li>Its observed event type is qualified with the same annotation
 * that (a) qualifies the event selector and (b) is, in turn,
 * qualified with {@link
 * KubernetesEventSelector @KubernetesEventSelector}.  For example, if
 * {@code @AllConfigMapEvents} appears on your event selector producer
 * method, then it should appear on your observer method's {@link
 * ConfigMap} parameter that is annotated with {@link
 * Observes @Observes}.</li>
 *
 * <li>Its observed event type is qualified with one of {@link
 * Added @Added}, {@link Modified @Modified} or {@link
 * Deleted @Deleted}.</li>
 *
 * <li>If you need access to the prior state of the Kubernetes
 * resource your observer method is observing, you may add it as a
 * standard (injected) parameter in the method's parameter list, but
 * it must be (a) qualified with the {@link Prior @Prior} annotation
 * and (b) of a type identical to that of the observed event type.
 * For example, if your observed event type is {@link ConfigMap}, then
 * your prior state parameter must also be of type {@link ConfigMap}
 * and must be annotated with {@link Prior @Prior}.</li>
 *
 * </ol>
 *
 * <p>Building upon the prior example, here is an example of an
 * observer method that is "paired" with the event selector above:</p>
 *
 * <blockquote><pre>private final void onConfigMapModification({@link Observes &#64;Observes} &#64;AllConfigMapEvents {@link Modified &#64;Modified} final {@link ConfigMap} configMap, {@link Prior &#64;Prior} final Optional&lt;{@link ConfigMap}&gt; prior) {
 *  assert configMap != null;
 *  // do something interesting with this modified {@link ConfigMap}
 *}</pre></blockquote>
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see AbstractBlockingExtension
 *
 * @see Controller
 *
 * @see KubernetesEventSelector
 *
 * @see Added
 *
 * @see Modified
 *
 * @see Deleted
 *
 * @see Prior
 */
public class KubernetesControllerExtension extends AbstractBlockingExtension {


  /*
   * Instance fields.
   */

  
  private final Collection<Controller<?>> controllers;
  
  private final Map<Set<Annotation>, Bean<?>> eventSelectorBeans;

  private final Set<Bean<?>> beans;

  private final Set<Class<? extends HasMetadata>> priorTypes;

  private boolean asyncNeeded;

  private boolean syncNeeded;
  
  private final PriorContext priorContext;

  private final KubernetesEventContext kubernetesEventContext;

  
  /*
   * Constructors.
   */

  
  /**
   * Creates a new {@link KubernetesControllerExtension}.
   *
   * @see #KubernetesControllerExtension(CountDownLatch)
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
   * @see #KubernetesControllerExtension()
   *
   * @see
   * AbstractBlockingExtension#AbstractBlockingExtension(CountDownLatch)
   */
  protected KubernetesControllerExtension(final CountDownLatch latch) {
    super(latch);
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, latch);
    }
    
    this.eventSelectorBeans = new HashMap<>();
    this.beans = new HashSet<>();
    this.priorTypes = new HashSet<>();
    this.controllers = new ArrayList<>();
    this.priorContext = new PriorContext();
    this.kubernetesEventContext = new KubernetesEventContext();

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */
  

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
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerMethod(@Observes final ProcessProducerMethod<X, ?> event,
                                                                                                                                                                          final BeanManager beanManager) {
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
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processProducerField(@Observes final ProcessProducerField<X, ?> event,
                                                                                                                                                                         final BeanManager beanManager) {
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
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processManagedBean(@Observes final ProcessManagedBean<X> event,
                                                                                                                                                                       final BeanManager beanManager) {
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
  private final <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<? extends HasMetadata>>> void processSyntheticBean(@Observes final ProcessSyntheticBean<X> event,
                                                                                                                                                                         final BeanManager beanManager) {
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

  private final <T extends HasMetadata> void validateScopeOfCacheBean(@Observes final ProcessBean<Map<Object, T>> event) {
    final String cn = this.getClass().getName();
    final String mn = "validateScopeOfCacheBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event });
    }
    if (event != null) {
      final Bean<?> bean = event.getBean();
      if (bean != null && !ApplicationScoped.class.equals(bean.getScope()) && this.logger.isLoggable(Level.WARNING)) {
        this.logger.logp(Level.WARNING, cn, mn, "{0} is not in application scope.", bean);
      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  private final <T extends HasMetadata> void validateScopeOfCacheBean(@Observes final ProcessProducerField<Map<Object, T>, ?> event) {
    final String cn = this.getClass().getName();
    final String mn = "validateScopeOfCacheBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event });
    }
    if (event != null) {
      final Bean<?> bean = event.getBean();
      if (bean != null && !ApplicationScoped.class.equals(bean.getScope()) && this.logger.isLoggable(Level.WARNING)) {
        this.logger.logp(Level.WARNING, cn, mn, "{0} is not in application scope.", bean);
      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  private final <T extends HasMetadata> void validateScopeOfCacheBean(@Observes final ProcessProducerMethod<Map<Object, T>, ?> event) {
    final String cn = this.getClass().getName();
    final String mn = "validateScopeOfCacheBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event });
    }
    if (event != null) {
      final Bean<?> bean = event.getBean();
      if (bean != null && !ApplicationScoped.class.equals(bean.getScope()) && this.logger.isLoggable(Level.WARNING)) {
        this.logger.logp(Level.WARNING, cn, mn, "{0} is not in application scope.", bean);
      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }
  
  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessObserverMethod} event and calls the {@link
   * #processPotentialEventSelectorObserverMethod(ProcessObserverMethod,
   * BeanManager)} method with the return value of the event's {@link
   * ProcessObserverMethod#getObserverMethod()} method and the
   * supplied {@link BeanManager}.
   *
   * @param <X> a type that extends {@link HasMetadata} and therefore
   * represents a persistent Kubernetes resource
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see
   * #processPotentialEventSelectorObserverMethod(ProcessObserverMethod,
   * BeanManager)
   */
  // Observer method processors are guaranteed by the specification to
  // be invoked after ProcessBean events.
  private final <X extends HasMetadata> void processObserverMethod(@Observes final ProcessObserverMethod<X, ?> event,
                                                                   final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    
    if (event != null) {
      this.processPotentialEventSelectorObserverMethod(event, beanManager);
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * {@linkplain Observes Observes} the supplied {@link
   * ProcessSyntheticObserverMethod} event and calls the {@link
   * #processPotentialEventSelectorObserverMethod(ProcessObserverMethod,
   * BeanManager)} method with the return value of the event's {@link
   * ProcessSyntheticObserverMethod#getObserverMethod()} method and
   * the supplied {@link BeanManager}.
   *
   * @param <X> a type that extends {@link HasMetadata} and therefore
   * represents a persistent Kubernetes resource
   *
   * @param event the container lifecycle event being observed; may be
   * {@code null} in which case no action will be performed
   *
   * @param beanManager the {@link BeanManager} for the current CDI
   * container; may be {@code null}
   *
   * @see
   * #processPotentialEventSelectorObserverMethod(ProcessObserverMethod,
   * BeanManager)
   */
  // Observer method processors are guaranteed by the specification to
  // be invoked after ProcessBean events.
  private final <X extends HasMetadata> void processSyntheticObserverMethod(@Observes final ProcessSyntheticObserverMethod<X, ?> event,
                                                                            final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processSyntheticObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }
    
    if (event != null) {
      this.processPotentialEventSelectorObserverMethod(event, beanManager);
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
      event.addContext(this.priorContext);
      event.addContext(this.kubernetesEventContext);
      
      this.eventSelectorBeans.clear();
      // TODO: consider: we have the ability to create Controller
      // beans here out of other bean raw materials
      // (e.g. appropriately-qualified knownObjects etc.).

      synchronized (this.priorTypes) {
        if (!this.priorTypes.isEmpty()) {
          for (final Type priorType : this.priorTypes) {
            assert priorType != null;

            event.addBean()
              // This Bean is never created via this (required by CDI)
              // callback; it is always supplied by
              // PriorContext#get(Bean), so the container will think
              // that it is eternal.
              .createWith(cc -> { throw new UnsupportedOperationException(); })
              .qualifiers(Prior.Literal.INSTANCE)
              .scope(PriorScoped.class)
              .types(new ParameterizedTypeImpl(null, Optional.class, new Type[] { priorType }));
            
          }
          this.priorTypes.clear();
        }
      }
      
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
  private final <T extends HasMetadata,
                 X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>>
                void startControllers(@Observes
                                      @Initialized(ApplicationScoped.class)
                                      @Priority(LIBRARY_AFTER)
                                      final Object ignored,
                                      final BeanManager beanManager) { 
    final String cn = this.getClass().getName();
    final String mn = "startControllers";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { ignored, beanManager });
    }

    if (beanManager != null && !this.beans.isEmpty()) {

      // Use the microbean-configuration-cdi library to abstract away
      // configuration details.  But we can't just put a
      // Configurations object in our incoming method parameters,
      // because according to the specification that will result in
      // non-portable behavior.  So we look it up "by hand".
      final Bean<?> configurationsBean = beanManager.resolve(beanManager.getBeans(Configurations.class));
      assert configurationsBean != null;
      final Configurations configurations =
        (Configurations)beanManager.getReference(configurationsBean,
                                                 Configurations.class,
                                                 beanManager.createCreationalContext(configurationsBean));
      assert configurations != null;      

      final Duration synchronizationInterval = configurations.getValue("synchronizationInterval", Duration.class);

      for (final Bean<?> bean : this.beans) {
        assert bean != null;
        
        final Set<Annotation> qualifiers = bean.getQualifiers();
        final Annotation[] qualifiersArray;
        if (qualifiers == null) {
          qualifiersArray = null;
        } else {
          qualifiersArray = qualifiers.toArray(new Annotation[qualifiers.size()]);
        }

        @Issue(id = "6", uri = "https://github.com/microbean/microbean-kubernetes-controller-cdi/issues/6")
        final Type cacheType = new ParameterizedTypeImpl(Map.class, new Type[] { Object.class, extractConcreteKubernetesResourceClass(bean) });

        final Map<Object, T> cache;
        final Set<Bean<?>> cacheBeans = beanManager.getBeans(cacheType, qualifiersArray);
        if (cacheBeans == null || cacheBeans.isEmpty()) {
          cache = null;
        } else {
          final Bean<?> cacheBean = beanManager.resolve(cacheBeans);
          if (cacheBean == null) {
            cache = null;
          } else {
            @SuppressWarnings("unchecked")
            final Map<Object, T> temp =
              (Map<Object, T>)beanManager.getReference(cacheBean,
                                                       cacheType,
                                                       beanManager.createCreationalContext(cacheBean));
            cache = temp;
          }
        }
        if (cache == null && this.logger.isLoggable(Level.INFO)) {
          this.logger.logp(Level.INFO, cn, mn,
                           "No Kubernetes resource cache found for qualifiers: {0}",
                           qualifiers);
        }
        
        final NotificationOptions notificationOptions;
        final Bean<?> notificationOptionsBean =
          beanManager.resolve(beanManager.getBeans(NotificationOptions.class, qualifiersArray));
        if (notificationOptionsBean == null) {
          notificationOptions = null;
        } else {
          notificationOptions =
            (NotificationOptions)beanManager.getReference(notificationOptionsBean,
                                                          NotificationOptions.class,
                                                          beanManager.createCreationalContext(notificationOptionsBean));
        }
        
        @SuppressWarnings("unchecked")
        final X contextualReference =
          (X)beanManager.getReference(bean,
                                      getListableVersionWatchableType(bean),
                                      beanManager.createCreationalContext(bean));

        final Controller<T> controller =
          new CDIController<>(contextualReference,
                              synchronizationInterval,
                              cache,
                              new CDIEventDistributor<>(this.priorContext,
                                                        this.kubernetesEventContext,
                                                        qualifiers,
                                                        notificationOptions,
                                                        this.syncNeeded,
                                                        this.asyncNeeded),
                              t -> {
                                if (this.logger.isLoggable(Level.SEVERE)) {
                                  this.logger.logp(Level.SEVERE, cn, mn, t.getMessage(), t);
                                }
                                return true;
                              });

        if (this.logger.isLoggable(Level.INFO)) {
          this.logger.logp(Level.INFO, cn, mn, "Starting {0}", controller);
        }
        try {
          controller.start();
        } catch (final IOException ioException) {
          throw new DeploymentException(ioException.getMessage(), ioException);
        }
        
        synchronized (this.controllers) {
          this.controllers.add(controller);
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
   * @param event the actual {@link BeforeDestroyed} event; ignored;
   * may be {@code null}
   *
   * @exception IOException if {@link Controller#close()} throws an
   * {@link IOException}
   *
   * @see #startControllers(Object, BeanManager)
   */
  private final void stopControllers(@Observes
                                     @BeforeDestroyed(ApplicationScoped.class)
                                     @Priority(LIBRARY_BEFORE)
                                     final Object event)
    throws IOException {
    final String cn = this.getClass().getName();
    final String mn = "stopControllers";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, event);
    }

    Exception exception = null;
    synchronized (this.controllers) {
      for (final Controller<?> controller : this.controllers) {
        assert controller != null;
        try {
          controller.close();
        } catch (final IOException | RuntimeException closeException) {
          if (exception == null) {
            exception = closeException;
          } else {
            exception.addSuppressed(closeException);
          }
        }
      }
    }

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
  private final void processPotentialEventSelectorBean(final Bean<?> bean, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processPotentialEventSelectorBean";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { bean, beanManager });
    }

    if (bean != null) {
      final Type listableVersionWatchableType = getListableVersionWatchableType(bean);
      if (listableVersionWatchableType != null) {
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
   * @param <X> a type that extends {@link HasMetadata} and therefore
   * represents a persistent Kubernetes resource
   *
   * @param event the {@link ProcessObserverMethod} event to inspect;
   * may be {@code null} in which case no action will be taken
   *
   * @param beanManager the {@link BeanManager} in effect for the
   * current CDI container; may be {@code null}
   *
   * @see Annotations#retainAnnotationsQualifiedWith(Collection,
   * Class, BeanManager)
   *
   * @see KubernetesEventSelector
   */
  private final <X extends HasMetadata> void processPotentialEventSelectorObserverMethod(final ProcessObserverMethod<X, ?> event, final BeanManager beanManager) {
    final String cn = this.getClass().getName();
    final String mn = "processPotentialEventSelectorObserverMethod";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { event, beanManager });
    }

    if (event != null) {
      final ObserverMethod<X> observerMethod = event.getObserverMethod();
      if (observerMethod != null) {
        final Set<Annotation> kubernetesEventSelectors = Annotations.retainAnnotationsQualifiedWith(observerMethod.getObservedQualifiers(), KubernetesEventSelector.class, beanManager);
        if (kubernetesEventSelectors != null && !kubernetesEventSelectors.isEmpty()) {
          event.configureObserverMethod()
            .notifyWith(new Notifier<>(this.priorContext, this.kubernetesEventContext, observerMethod));
          if (observerMethod.isAsync()) {
            if (!this.asyncNeeded) {
              this.asyncNeeded = true;
            }
          } else if (!this.syncNeeded) {
            this.syncNeeded = true;
          }
          final Bean<?> bean;
          synchronized (this.eventSelectorBeans) {
            bean = this.eventSelectorBeans.remove(kubernetesEventSelectors);
          }
          if (bean != null) {
            boolean added;
            synchronized (this.beans) {            
              added = this.beans.add(bean);
            }
            if (added) {
              final Class<? extends HasMetadata> concreteKubernetesResourceClass = extractConcreteKubernetesResourceClass(bean);
              assert concreteKubernetesResourceClass != null;
              synchronized (this.priorTypes) {
                this.priorTypes.add(concreteKubernetesResourceClass);
              }
            }
          }
        }
      }
    }

    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }
  

  /*
   * Static methods.
   */

  
  /**
   * A bit of a hack to return the {@link Type} that is the "right
   * kind" of {@link Listable} and {@link VersionWatchable}
   * implementation from the supplied {@link Bean}'s {@linkplain
   * Bean#getTypes() types}, if it is present among them, and to
   * return {@code null} otherwise.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>{@link Operation Operation.class} is the most general
   * interface that implements both {@link Listable} and {@link
   * VersionWatchable}, which is a common constraint for {@link
   * Controller} operations, and is often what this method returns in
   * {@link ParameterizedType} form.
   *
   * @param bean the {@link Bean} to inspect; may be {@code null} in
   * which case {@code null} will be returned
   *
   * @return a {@link Type} that is both {@link Listable} and {@link
   * VersionWatchable}, or {@code null}
   *
   * @see Operation
   *
   * @see Listable
   *
   * @see VersionWatchable
   */
  private static final Type getListableVersionWatchableType(final Bean<?> bean) {
    final String cn = KubernetesControllerExtension.class.getName();
    final Logger logger = Logger.getLogger(cn);
    assert logger != null;
    final String mn = "getListableVersionWatchableType";
    if (logger.isLoggable(Level.FINER)) {
      logger.entering(cn, mn, bean);
    }
    
    final Type returnValue;
    if (bean == null) {
      returnValue = null;
    } else {
      returnValue = getListableVersionWatchableType(bean.getTypes());
    }
    
    if (logger.isLoggable(Level.FINER)) {
      logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  private static final Type getListableVersionWatchableType(final Collection<? extends Type> beanTypes) {
    final String cn = KubernetesControllerExtension.class.getName();
    final Logger logger = Logger.getLogger(cn);
    assert logger != null;
    final String mn = "getListableVersionWatchableType";
    if (logger.isLoggable(Level.FINER)) {
      logger.entering(cn, mn, beanTypes);
    }

    final Type returnValue;
    if (beanTypes == null || beanTypes.isEmpty()) {
      returnValue = null;
    } else {
      Type candidate = null;
      for (final Type beanType : beanTypes) {
        if (beanType instanceof ParameterizedType) {
          candidate = getListableVersionWatchableType((ParameterizedType)beanType);
          if (candidate != null) {
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

  private static final Type getListableVersionWatchableType(final ParameterizedType type) {
    final String cn = KubernetesControllerExtension.class.getName();
    final Logger logger = Logger.getLogger(cn);
    assert logger != null;
    final String mn = "getListableVersionWatchableType";
    if (logger.isLoggable(Level.FINER)) {
      logger.entering(cn, mn, type);
    }

    Type candidate = null;
    final Type rawType = type.getRawType();
    if (rawType instanceof Class) {
      // This should always be the case; see e.g. https://stackoverflow.com/a/5767681/208288
      final Class<?> rawClass = (Class<?>)rawType;
      if (Listable.class.isAssignableFrom(rawClass) && VersionWatchable.class.isAssignableFrom(rawClass)) {
        // TODO: check type parameters
        candidate = type;
      }
    }
    
    final Type returnValue = candidate;

    if (logger.isLoggable(Level.FINER)) {
      logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  private static final Class<? extends HasMetadata> extractConcreteKubernetesResourceClass(final BeanAttributes<?> beanAttributes) {
    Class<? extends HasMetadata> returnValue = null;
    if (beanAttributes != null) {
      returnValue = extractConcreteKubernetesResourceClass(beanAttributes.getTypes());
    }
    return returnValue;
  }

  private static final Class<? extends HasMetadata> extractConcreteKubernetesResourceClass(final Set<? extends Type> types) {
    Class<? extends HasMetadata> returnValue = null;
    if (types != null && !types.isEmpty()) {
      final Set<Type> typesToProcess = new LinkedHashSet<>(types);
      while (!typesToProcess.isEmpty()) {
        final Iterator<Type> iterator = typesToProcess.iterator();
        assert iterator != null;
        assert iterator.hasNext();
        final Type type = iterator.next();
        iterator.remove();
        if (type != null) {
          if (type instanceof Class<?>) {
            final Class<?> concreteClass = (Class<?>)type;
            if (HasMetadata.class.isAssignableFrom(concreteClass)) {
              @SuppressWarnings("unchecked")
              final Class<? extends HasMetadata> temp = (Class<? extends HasMetadata>)concreteClass;
              returnValue = temp;
              break;
            }
          } else if (type instanceof ParameterizedType) {
            final ParameterizedType pType = (ParameterizedType)type;
            final Type[] actualTypeArguments = pType.getActualTypeArguments();
            if (actualTypeArguments != null && actualTypeArguments.length > 0) {
              for (final Type actualTypeArgument : actualTypeArguments) {
                if (actualTypeArgument != null) {
                  typesToProcess.add(actualTypeArgument);
                }
              }
            }
          }
        }
      }
    }
    return returnValue;
  }


  /*
   * Inner and nested classes.
   */

  
  private static final class ParameterizedTypeImpl implements ParameterizedType {

    private final Type ownerType;

    private final Type rawType;

    private final Type[] actualTypeArguments;

    private final int hashCode;

    private ParameterizedTypeImpl(final Class<?> rawType, final Type[] actualTypeArguments) {
      this(null, rawType, actualTypeArguments);
    }
    
    private ParameterizedTypeImpl(final Type ownerType, final Class<?> rawType, final Type[] actualTypeArguments) {
      super();
      this.ownerType = ownerType;
      this.rawType = Objects.requireNonNull(rawType);
      this.actualTypeArguments = actualTypeArguments;
      this.hashCode = this.computeHashCode();
    }
    
    @Override
    public final Type getOwnerType() {
      return this.ownerType;
    }
    
    @Override
    public final Type getRawType() {
      return this.rawType;
    }
    
    @Override
    public final Type[] getActualTypeArguments() {
      return this.actualTypeArguments;
    }
    
    @Override
    public final int hashCode() {
      return this.hashCode;
    }

    private final int computeHashCode() {
      int hashCode = 17;
      
      final Object ownerType = this.getOwnerType();
      int c = ownerType == null ? 0 : ownerType.hashCode();
      hashCode = 37 * hashCode + c;
      
      final Object rawType = this.getRawType();
      c = rawType == null ? 0 : rawType.hashCode();
      hashCode = 37 * hashCode + c;
      
      final Type[] actualTypeArguments = this.getActualTypeArguments();
      c = Arrays.hashCode(actualTypeArguments);
      hashCode = 37 * hashCode + c;
      
      return hashCode;
    }
    
    @Override
    public final boolean equals(final Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof ParameterizedType) {
        final ParameterizedType her = (ParameterizedType)other;
        
        final Object ownerType = this.getOwnerType();
        if (ownerType == null) {
          if (her.getOwnerType() != null) {
            return false;
          }
        } else if (!ownerType.equals(her.getOwnerType())) {
          return false;
        }
        
        final Object rawType = this.getRawType();
        if (rawType == null) {
          if (her.getRawType() != null) {
            return false;
          }
        } else if (!rawType.equals(her.getRawType())) {
          return false;
        }
        
        final Type[] actualTypeArguments = this.getActualTypeArguments();
        if (!Arrays.equals(actualTypeArguments, her.getActualTypeArguments())) {
          return false;
        }
        
        return true;
      } else {
        return false;
      }
    }

  }

  private static final class CDIController<T extends HasMetadata> extends Controller<T> {

    private final EventDistributor<T> eventDistributor;

    private final boolean close;

    // This @SuppressWarnings("rawtypes") is here because the
    // kubernetes-model project uses raw types throughout.  This class
    // does not.
    @SuppressWarnings("rawtypes")
    private
    <X extends Listable<? extends KubernetesResourceList>
               & VersionWatchable<? extends Closeable, Watcher<T>>>
    CDIController(final X operation,
                  final Duration synchronizationInterval,
                  final Map<Object, T> knownObjects,
                  final CDIEventDistributor<T> eventDistributor,
                  final Function<? super Throwable, Boolean> errorHandler) {
      this(operation, synchronizationInterval, errorHandler, knownObjects, new EventDistributor<>(knownObjects, synchronizationInterval), true);
      assert this.eventDistributor != null;
      if (eventDistributor != null) {
        this.eventDistributor.addConsumer(eventDistributor, errorHandler);
      }      
    }

    // This @SuppressWarnings("rawtypes") is here because the
    // kubernetes-model project uses raw types throughout.  This class
    // does not.
    @SuppressWarnings("rawtypes")
    private
    <X extends Listable<? extends KubernetesResourceList>
               & VersionWatchable<? extends Closeable, Watcher<T>>>
    CDIController(final X operation,
                  final Duration synchronizationInterval,
                  final Function<? super Throwable, Boolean> errorHandler,
                  final Map<Object, T> knownObjects,
                  final EventDistributor<T> siphon,
                  final boolean close) {
      super(operation, null, synchronizationInterval, errorHandler, knownObjects, siphon);
      this.eventDistributor = Objects.requireNonNull(siphon);
      this.close = close;
    }

    @Override
    protected final boolean shouldSynchronize() {
      return this.eventDistributor.shouldSynchronize();
    }

    @Override
    protected final void onClose() {
      if (this.close) {
        this.eventDistributor.close();
      }
    }
    
  }
  
  private static final class CDIEventDistributor<T extends HasMetadata> implements Consumer<AbstractEvent<? extends T>> {

    private static final Annotation[] EMPTY_ANNOTATION_ARRAY = new Annotation[0];

    private final PriorContext priorContext;

    private final KubernetesEventContext kubernetesEventContext;
    
    private final Annotation[] qualifiers;

    private final NotificationOptions notificationOptions;

    private final boolean syncNeeded;

    private final boolean asyncNeeded;

    private final Logger logger;
    
    private CDIEventDistributor(final PriorContext priorContext,
                                final KubernetesEventContext kubernetesEventContext,
                                final Set<Annotation> qualifiers,
                                final NotificationOptions notificationOptions,
                                final boolean syncNeeded,
                                final boolean asyncNeeded) {
      super();
      final String cn = this.getClass().getName();      
      this.logger = Logger.getLogger(cn);
      assert this.logger != null;
      final String mn = "<init>";
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn,
                             new Object[] { priorContext,
                                            kubernetesEventContext,
                                            qualifiers,
                                            notificationOptions,
                                            Boolean.valueOf(syncNeeded),
                                            Boolean.valueOf(asyncNeeded)
                             });
      }

      this.priorContext = Objects.requireNonNull(priorContext);
      this.kubernetesEventContext = Objects.requireNonNull(kubernetesEventContext);
      if (qualifiers == null) {
        this.qualifiers = EMPTY_ANNOTATION_ARRAY;
      } else {
        this.qualifiers = qualifiers.toArray(new Annotation[qualifiers.size()]);
      }
      this.notificationOptions = notificationOptions;
      this.syncNeeded = syncNeeded;
      this.asyncNeeded = asyncNeeded;

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

      if (controllerEvent != null && (this.syncNeeded || this.asyncNeeded)) {

        final BeanManager beanManager = CDI.current().getBeanManager();
        assert beanManager != null;

        final javax.enterprise.event.Event<Object> cdiEventMachinery = beanManager.getEvent();
        assert cdiEventMachinery != null;

        // Copy the qualifiers we were supplied with into an array big
        // enough to hold one more qualifier.  That qualifier will be
        // based on the event type, which of course we didn't know at
        // construction time.
        final Annotation[] qualifiers = Arrays.copyOf(this.qualifiers, this.qualifiers.length + 1);
        assert qualifiers != null;

        final AbstractEvent.Type eventType = controllerEvent.getType();
        assert eventType != null;

        switch (eventType) {
          
        case ADDITION:
          if (controllerEvent instanceof SynchronizationEvent) {
            qualifiers[qualifiers.length - 1] = Added.Literal.withSynchronization();
          } else {
            qualifiers[qualifiers.length - 1] = Added.Literal.withoutSynchronization();
          }
          break;
          
        case MODIFICATION:
          if (controllerEvent instanceof SynchronizationEvent) {
            qualifiers[qualifiers.length - 1] = Modified.Literal.withSynchronization();
          } else {
            qualifiers[qualifiers.length - 1] = Modified.Literal.withoutSynchronization();
          }
          break;
          
        case DELETION:
          assert !(controllerEvent instanceof SynchronizationEvent);
          qualifiers[qualifiers.length - 1] = Deleted.Literal.INSTANCE;
          break;
          
        default:
          throw new IllegalStateException();
          
        }

        // This resource will be the actual "event" we end up firing.
        final T resource = controllerEvent.getResource();
        assert resource != null;

        // The "prior resource" represents the prior state (if any)
        // and can be null.  We'll arrange for this to be "created" by
        // our PriorContext CDI Context when observer methods contain
        // a parameter qualified with @Prior.
        this.priorContext.put(resource, Optional.ofNullable(controllerEvent.getPriorResource()));

        @SuppressWarnings("unchecked")
        final javax.enterprise.event.Event<T> broadcaster = cdiEventMachinery.select((Class<T>)resource.getClass(), qualifiers);

        if (this.asyncNeeded) {

          // Set up the machinery to fire the event asynchronously,
          // possibly in parallel.
          
          final CompletionStage<T> stage;
          if (this.notificationOptions == null) {
            stage = broadcaster.fireAsync(resource);
          } else {
            stage = broadcaster.fireAsync(resource, this.notificationOptions);
          }
          assert stage != null;

          // When all asynchronous observers have been notified, then
          // fire synchronous events (if needed).  Ensure that the
          // PriorContext that is responsible for supplying injected
          // observer method parameters annotated with @Prior is
          // deactivated in all cases.

          // TODO: should we make it configurable whether to fire
          // synchronous events before asynchronous events or the
          // other way around?
          
          stage.whenComplete((event, throwable) -> {
              if (throwable != null && this.logger.isLoggable(Level.SEVERE)) {
                logger.logp(Level.SEVERE, cn, mn, throwable.getMessage(), throwable);
              }
              // TODO: should the presence of a non-null throwable
              // cause us to not perform synchronous firing?
              try {
                assert event != null;
                if (this.syncNeeded) {
                  broadcaster.fire(event);
                }
              } finally {
                this.kubernetesEventContext.destroy();
                this.priorContext.remove(event);
              }
            });
          
        } else {
          assert this.syncNeeded;

          try {
            broadcaster.fire(resource);
          } finally {
            this.kubernetesEventContext.destroy();
            this.priorContext.remove(resource);
          }
        }
        
      }

      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.exiting(cn, mn);
      }
    }
    
  }

  private static final class PriorContext implements AlterableContext {

    private static final InheritableThreadLocal<CurrentEventContext> currentEventContext = new InheritableThreadLocal<CurrentEventContext>() {
        @Override
        protected final CurrentEventContext initialValue() {
          return new CurrentEventContext();
        }
      };
    
    /**
     * A {@linkplain Collections#synchronizedMap(Map) synchronized}
     * {@link IdentityHashMap} that maps a "current" {@link
     * HasMetadata} to its prior representation.
     *
     * @see #put(HasMetadata, Optional)
     */
    private final Map<HasMetadata, Optional<? extends HasMetadata>> instances;
    
    private PriorContext() {
      super();
      // This needs to be an IdentityHashMap under the covers because
      // it turns out that all kubernetes-model classes use Lombok's
      // indiscriminate equals()-and-hashCode() generation.  We need
      // to track Kubernetes resources in this Context implementation
      // by their actual JVM identity.
      this.instances = Collections.synchronizedMap(new IdentityHashMap<>());
    }

    /**
     * Activates this {@link PriorContext} <strong>for the {@linkplain
     * Thread#currentThread() current <code>Thread</code>}</strong>.
     *
     * @param currentEvent the {@link HasMetadata} that is currently
     * being fired as a CDI event; must not be {@code null}
     *
     * @exception NullPointerException if {@code currentEvent} is
     * {@code null}
     */
    private final void activate(final HasMetadata currentEvent) {
      Objects.requireNonNull(currentEvent);
      final CurrentEventContext c = currentEventContext.get();
      assert c != null;
      c.currentEvent = currentEvent;
      c.active = true;
    }

    /**
     * Deactivates this {@link PriorContext} <strong>for the {@linkplain
     * Thread#currentThread() current <code>Thread</code>}</strong>.
     */
    private final void deactivate() {
      final CurrentEventContext c = currentEventContext.get();
      assert c != null;
      c.active = false;
      c.currentEvent = null;
      // Note: do NOT be tempted to call this.remove() here.
    }

    /**
     * Associates the supplied {@code priorEvent} with the supplied
     * {@code currentEvent} and returns any previously associated
     * event.
     *
     * <p>This method <strong>may return {@code null}</strong>.</p>
     *
     * @param <X> a type that extends {@link HasMetadata} and therefore
     * represents a persistent Kubernetes resource
     *
     * @param currentEvent a Kubernetes resource about to be fired as
     * a CDI event; must not be {@code null}
     *
     * @param priorEvent an {@link Optional} Kubernetes resource that
     * represents the last known state of the {@code currentEvent}
     * Kubernetes resource; must not be {@code null}
     *
     * @return any previously associated Kubernetes resource as an
     * {@link Optional}, <strong>or, somewhat unusually, {@code null}
     * if there was no such {@link Optional}</strong>
     *
     * @exception NullPointerException if {@code currentEvent} or
     * {@code priorEvent} is {@code null}
     */
    private final <X extends HasMetadata> Optional<X> put(final X currentEvent, final Optional<X> priorEvent) {
      @SuppressWarnings("unchecked")
      final Optional<X> returnValue = (Optional<X>)this.instances.put(Objects.requireNonNull(currentEvent),
                                                                      Objects.requireNonNull(priorEvent));
      return returnValue;
    }

    /**
     * Removes the supplied {@link HasMetadata} from this {@link
     * PriorContext}'s registry of such objects and returns any {@link
     * Optional} indexed under it.
     *
     * <p>This method <strong>may return {@code null}</strong>.</p>
     *
     * @param currentEvent the {@link HasMetadata} to remove; must not
     * be {@code null}
     *
     * @return an {@link Optional} representing the prior state
     * indexed under the supplied {@link HasMetadata}, <strong>or
     * {@code null}</strong>
     *
     * @exception NullPointerException if {@code currentEvent} is
     * {@code null}
     */
    private final Optional<? extends HasMetadata> remove(final HasMetadata currentEvent) {
      return this.instances.remove(Objects.requireNonNull(currentEvent));
    }

    private final Optional<? extends HasMetadata> get() {
      if (!this.isActive()) {
        throw new ContextNotActiveException();
      }
      final CurrentEventContext c = currentEventContext.get();
      assert c != null;
      assert c.active;
      assert c.currentEvent != null;
      // Yes, this can return null, and yes, our return type is
      // Optional.  Do NOT be tempted to return an empty Optional
      // here!
      return this.instances.get(c.currentEvent);
    }
    
    @Override
    public final <T> T get(final Contextual<T> bean) {
      @SuppressWarnings("unchecked")
      final T returnValue = (T)this.get();
      return returnValue;
    }

    @Override
    public final <T> T get(final Contextual<T> bean, final CreationalContext<T> cc) {
      @SuppressWarnings("unchecked")
      final T returnValue = (T)this.get();
      return returnValue;
    }

    @Override
    public final void destroy(final Contextual<?> bean) {
      if (!this.isActive()) {
        throw new ContextNotActiveException();
      }
      final CurrentEventContext c = currentEventContext.get();
      assert c != null;
      assert c.active;
      assert c.currentEvent != null;
      this.remove(c.currentEvent);
    }

    @Override
    public final Class<? extends Annotation> getScope() {
      return PriorScoped.class;
    }

    @Override
    public final boolean isActive() {
      final CurrentEventContext c = currentEventContext.get();
      assert c != null;
      return c.active && c.currentEvent != null && this.instances.containsKey(c.currentEvent);
    }

    private static final class CurrentEventContext {

      private volatile HasMetadata currentEvent;

      private volatile boolean active;

      private CurrentEventContext() {
        super();
      }
      
    }
    
  }

  private static final class Notifier<T extends HasMetadata> implements EventConsumer<T> {

    private final PriorContext priorContext;

    private final KubernetesEventContext kubernetesEventContext;
    
    private final ObserverMethod<T> observerMethod;
    
    private Notifier(final PriorContext priorContext,
                     final KubernetesEventContext kubernetesEventContext,
                     final ObserverMethod<T> observerMethod) {
      super();
      this.priorContext = Objects.requireNonNull(priorContext);
      this.kubernetesEventContext = Objects.requireNonNull(kubernetesEventContext);
      this.observerMethod = Objects.requireNonNull(observerMethod);
    }

    @Override
    public final void accept(final EventContext<T> eventContext) {
      try {
        this.kubernetesEventContext.setActive(true);
        this.priorContext.activate(Objects.requireNonNull(eventContext).getEvent()); // thread-specific
        this.observerMethod.notify(eventContext);
      } finally {
        this.priorContext.deactivate(); // thread-specific
        this.kubernetesEventContext.setActive(false);
      }
    }
    
  }

  @Retention(value = RetentionPolicy.RUNTIME)
  @Scope // deliberately NOT NormalScope
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD })
  private static @interface PriorScoped {

  }

}
