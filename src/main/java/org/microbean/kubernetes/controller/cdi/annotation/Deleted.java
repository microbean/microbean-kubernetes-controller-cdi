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
 * <em>deletion</em> of certain Kubernetes resources.
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
public @interface Deleted {


  /*
   * Inner and nested classes.
   */


  /**
   * An {@link AnnotationLiteral} that implements {@link Deleted}.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   */
  public static final class Literal extends AnnotationLiteral<Deleted> implements Deleted {


    /*
     * Static fields.
     */


    private static final long serialVersionUID = 1L;

    /**
     * A {@link Deleted} instance.
     *
     * <p>This field is never {@code null}.</p>
     */
    public static final Deleted INSTANCE = new Literal();

  }

}
