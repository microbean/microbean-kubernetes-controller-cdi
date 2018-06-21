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

import javax.inject.Qualifier;

import javax.enterprise.util.AnnotationLiteral;

/**
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 */
@Documented
@Qualifier
@Retention(value = RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER })
public @interface Modification {

  boolean synchronization() default false;
  
  public static final class Literal extends AnnotationLiteral<Modification> implements Modification {
    
    private static final long serialVersionUID = 1L;
    
    private static final Modification WITH_SYNCHRONIZATION = new Literal(true);

    private static final Modification WITHOUT_SYNCHRONIZATION = new Literal(false);
    
    private final boolean synchronization;
    
    private Literal(final boolean synchronization) {
      super();
      this.synchronization = synchronization;
    }
    
    @Override
    public final boolean synchronization() {
      return this.synchronization;
    }
    
    public static Modification withSynchronization() {
      return WITH_SYNCHRONIZATION;
    }

    public static Modification withoutSynchronization() {
      return WITHOUT_SYNCHRONIZATION;
    }
    
  }
  
}
