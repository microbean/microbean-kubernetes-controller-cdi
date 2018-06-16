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

/**
 * Provides classes and interfaces that help with writing <a
 * href="https://engineering.bitnami.com/articles/a-deep-dive-into-kubernetes-controllers.html">Kubernetes
 * controllers</a> in terms of <a href="http://cdi-spec.org/">CDI</a>
 * <a href="https://www.pavel.cool/javaee/cdi-events/">events</a>.
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see
 * org.microbean.kubernetes.controller.cdi.KubernetesControllerExtension
 *
 * @see
 * org.microbean.kubernetes.controller.cdi.KubernetesEventSelector
 */
package org.microbean.kubernetes.controller.cdi;
