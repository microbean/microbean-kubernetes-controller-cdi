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

import java.lang.annotation.Annotation;

import java.util.Objects;

import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ContextNotActiveException;

import javax.enterprise.context.spi.AlterableContext;
import javax.enterprise.context.spi.Contextual;
import javax.enterprise.context.spi.CreationalContext;

import org.microbean.kubernetes.controller.cdi.annotation.KubernetesEventScoped;

class KubernetesEventContext implements AlterableContext {

  private static final InheritableThreadLocal<Scope> scope = new InheritableThreadLocal<Scope>() {
      @Override
      protected final Scope initialValue() {
        return new Scope(KubernetesEventScoped.class);
      }
    };
  
  KubernetesEventContext() {
    super();
  }
  
  @Override
  public final Class<? extends Annotation> getScope() {
    return scope.get().getScope();
  }
  
  @Override
  public boolean isActive() {
    return scope.get().isActive();
  }

  void setActive(final boolean active) {
    scope.get().setActive(active);
  }

  @Override
  public final <T> T get(final Contextual<T> contextual) {
    return scope.get().get(contextual);
  }
  
  @Override
  public final <T> T get(final Contextual<T> contextual, final CreationalContext<T> creationalContext) {
    return scope.get().get(contextual, creationalContext);
  }

  @Override
  public final void destroy(final Contextual<?> contextual) {
    scope.get().destroy(contextual);
  }

  void destroy() {
    scope.get().destroy();
  }

  private static final class Scope implements AlterableContext {

    private final Class<? extends Annotation> scopeAnnotation;
    
    private final ConcurrentHashMap<Contextual<?>, Instance<?>> instances;

    private boolean active;
    
    private Scope(final Class<? extends Annotation> scopeAnnotation) {
      super();
      this.scopeAnnotation = Objects.requireNonNull(scopeAnnotation);
      this.instances = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isActive() {
      return active;
    }

    private void setActive(final boolean active) {
      this.active = active;
    }

    @Override
    public final <T> T get(final Contextual<T> contextual) {
      if (!this.isActive()) {
        throw new ContextNotActiveException();
      }
      final Instance<?> instance = this.instances.get(Objects.requireNonNull(contextual));
      final T returnValue;
      if (instance == null) {
        returnValue = null;
      } else {
        @SuppressWarnings("unchecked")
        final T temp = (T)instance.getInstance();
        returnValue = temp;
      }
      return returnValue;
    }
    
    @Override
    public final <T> T get(final Contextual<T> contextual, final CreationalContext<T> creationalContext) {
      if (!this.isActive()) {
        throw new ContextNotActiveException();
      }
      @SuppressWarnings("unchecked")
      final T temp = (T)this.instances.computeIfAbsent(contextual, c -> new Instance<>(Objects.requireNonNull(contextual), creationalContext)).getInstance();
      return temp;
    }
    
    @Override
    public final void destroy(final Contextual<?> contextual) {
      if (!this.isActive()) {
        throw new ContextNotActiveException();
      }
      final Instance<?> instance = this.instances.get(contextual);
      if (instance != null) {
        instance.destroy();
      }
    }

    private void destroy() {
      this.active = false; // prevent new 
      this.instances.forEachValue(Long.MAX_VALUE, v -> v.destroy());
      this.instances.clear();
    }
    
    @Override
    public final Class<? extends Annotation> getScope() {
      return this.scopeAnnotation;
    }
    
    private static final class Instance<T> {
      
      private final T instance;
      
      private final CreationalContext<T> creationalContext;
      
      private final Contextual<T> contextual;
      
      private Instance(final Contextual<T> contextual, final CreationalContext<T> creationalContext) {
        this(contextual, creationalContext, Objects.requireNonNull(contextual).create(creationalContext));
      }
      
      private Instance(final Contextual<T> contextual, final CreationalContext<T> creationalContext, final T instance) {
        super();
        this.contextual = contextual;
        this.creationalContext = creationalContext;
        this.instance = instance;
      }
      
      private final T getInstance() {
        return this.instance;
      }
      
      private final void destroy() {        
        this.contextual.destroy(this.instance, this.creationalContext);
      }
      
    }
    
  }
  
}
