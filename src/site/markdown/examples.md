# Examples

Here is an example Kubernetes controller implemented using this framework:

```
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
package org.microbean.kubernetes.controller.cdi.examples;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;

import javax.enterprise.event.Observes;

import javax.enterprise.inject.Produces;

import javax.inject.Qualifier;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;

import io.fabric8.kubernetes.client.KubernetesClient;

import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.fabric8.kubernetes.client.internal.PatchUtils;

import io.fabric8.zjsonpatch.JsonDiff;

import org.microbean.kubernetes.controller.cdi.annotation.Added;
import org.microbean.kubernetes.controller.cdi.annotation.Deleted; // for javadoc only
import org.microbean.kubernetes.controller.cdi.annotation.KubernetesEventSelector;
import org.microbean.kubernetes.controller.cdi.annotation.Modified;
import org.microbean.kubernetes.controller.cdi.annotation.Prior;

@ApplicationScoped
final class AllDefaultConfigMapsController {

  private static final Logger logger = Logger.getLogger(AllDefaultConfigMapsController.class.getName());

  private AllDefaultConfigMapsController() {
    super();
  }

  @Produces
  @ApplicationScoped
  @AllDefaultConfigMaps
  private static final NonNamespaceOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> produceAllDefaultConfigMaps(final KubernetesClient client) {
    return client.configMaps().inNamespace("default");
  }

  private final void onConfigMapAddition(@Observes
                                         @AllDefaultConfigMaps
                                         @Added
                                         final ConfigMap newConfigMap) {
    Objects.requireNonNull(newConfigMap);
    
    // TODO: whatever you like
    
  }

  private final void onConfigMapModification(@Observes
                                             @AllDefaultConfigMaps
                                             @Modified
                                             final ConfigMap modifiedConfigMap,
                                             @Prior
                                             final Optional<ConfigMap> priorConfigMap) {
    Objects.requireNonNull(modifiedConfigMap);
    Objects.requireNonNull(priorConfigMap);

    // TODO: whatever you like

  }

  @Produces
  @ApplicationScoped
  @AllDefaultConfigMaps
  private static final Map<Object, ConfigMap> produceKubernetesResourceCache() {
    return new HashMap<>();
  }

  @Documented
  @KubernetesEventSelector
  @Qualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE })
  public @interface AllDefaultConfigMaps {

  }
  
}

```
