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

import javax.enterprise.context.ApplicationScoped;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;

import javax.inject.Inject;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Operation;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;

@ApplicationScoped
public class KubernetesControllerMachine {

  @Inject
  @SuppressWarnings("rawtypes")
  public KubernetesControllerMachine(@Any final Instance<Operation<? extends HasMetadata, ? extends KubernetesResourceList, ?, ?>> stuff) {
    super();
    for (final Object op : stuff) {
      System.out.println("  *** op: " + op);
    }
  }
  
  
}
