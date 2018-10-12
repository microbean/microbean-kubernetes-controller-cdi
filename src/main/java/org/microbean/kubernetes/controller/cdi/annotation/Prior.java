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

import java.lang.reflect.ParameterizedType; // for javadoc only

import java.util.Optional; // for javadoc only

import javax.inject.Qualifier;

import javax.enterprise.util.AnnotationLiteral;

import io.fabric8.kubernetes.api.model.HasMetadata; // for javadoc only

/**
 * <strong>An extraordinarily special-purpose {@link Qualifier}
 * annotation</strong> that may be used <em>only</em> to qualify a
 * parameter in an observer method that meets the following criteria:
 *
 * <ol>
 *
 * <li>The observer method's <a
 * href="http://docs.jboss.org/cdi/spec/2.0/cdi-spec.html#events">observed
 * event type</a> is an instance of {@link HasMetadata}.</li>
 *
 * <li>The observer method's <a
 * href="http://docs.jboss.org/cdi/spec/2.0/cdi-spec.html#events">observed
 * event type</a> parameter is qualified with an annotation on which
 * {@link KubernetesEventSelector} and {@link Qualifier} appear.</li>
 *
 * <li>The type of the parameter in question is a parameterized type
 * whose {@linkplain ParameterizedType#getRawType() raw type} is
 * {@link Optional Optional} and whose sole {@linkplain
 * ParameterizedType#getActualTypeArguments() actual type argument} is
 * <em>identical</em> to the observer method's <a
 * href="http://docs.jboss.org/cdi/spec/2.0/cdi-spec.html#events">observed
 * event type</a>.</li>
 *
 * </ol>
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 */
@Documented
@Qualifier
@Retention(value = RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER })
public @interface Prior {

  public static final class Literal extends AnnotationLiteral<Prior> implements Prior {

    private static final long serialVersionUID = 1L;
    
    public static final Prior INSTANCE = new Literal();
    
  }
  
}
