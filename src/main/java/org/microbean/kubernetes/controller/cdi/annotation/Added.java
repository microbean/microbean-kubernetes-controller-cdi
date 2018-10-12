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
package org.microbean.kubernetes.controller.cdi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import javax.enterprise.util.AnnotationLiteral;

/**
 * A {@link Qualifier} that can be used only on an observer method's
 * <a
 * href="http://docs.jboss.org/cdi/spec/2.0/cdi-spec.html#observer_method_event_parameter">event
 * parameter</a> that is also annotated with an annotation annotated
 * with {@link KubernetesEventSelector}, thus indicating that the
 * observer method is interested in being notified of the
 * <em>addition</em> of certain Kubernetes resources.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see KubernetesEventSelector
 */
@Documented
@Qualifier
@Retention(value = RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER })
public @interface Added {

  /**
   * Whether the observer method should be notified in the case of
   * <em>synchronization events</em>.
   *
   * @return true if the observer method should be notified in the case of
   * <em>synchronization events</em>, {@code false} otherwise
   */
  boolean synchronization() default false;


  /*
   * Inner and nested classes.
   */


  /**
   * An {@link AnnotationLiteral} that implements {@link Added}.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   */
  public static final class Literal extends AnnotationLiteral<Added> implements Added {


    /*
     * Static fields.
     */


    private static final long serialVersionUID = 1L;

    private static final Added WITH_SYNCHRONIZATION = new Literal(true);

    private static final Added WITHOUT_SYNCHRONIZATION = new Literal(false);


    /*
     * Instance fields.
     */


    private final boolean synchronization;


    /*
     * Constructors.
     */


    private Literal(final boolean synchronization) {
      super();
      this.synchronization = synchronization;
    }


    /*
     * Instance methods.
     */


    /**
     * Whether the observer method should be notified in the case of
     * <em>synchronization events</em>.
     *
     * @return true if the observer method should be notified in the case of
     * <em>synchronization events</em>, {@code false} otherwise
     */
    @Override
    public final boolean synchronization() {
      return this.synchronization;
    }


    /*
     * Static methods.
     */


    /**
     * Returns an {@link Added} instance whose {@link
     * #synchronization() synchronization} element is {@code true}.
     *
     * <p>This method never returns {@code null}.</p>
     *
     * @return a non-{@code null} {@link Added}
     */
    public static Added withSynchronization() {
      return WITH_SYNCHRONIZATION;
    }

    /**
     * Returns an {@link Added} instance whose {@link
     * #synchronization() synchronization} element is {@code false}.
     *
     * <p>This method never returns {@code null}.</p>
     *
     * @return a non-{@code null} {@link Added}
     */
    public static Added withoutSynchronization() {
      return WITHOUT_SYNCHRONIZATION;
    }

  }

}
