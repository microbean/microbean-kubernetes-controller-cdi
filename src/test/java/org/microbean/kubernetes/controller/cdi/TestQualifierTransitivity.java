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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.lang.reflect.AnnotatedElement;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.BeanManager;

import javax.inject.Qualifier;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestQualifierTransitivity {

  public TestQualifierTransitivity() {
    super();
  }

  @Test
  public void testNonInheritedMetaQualifierAbsent() throws Exception {
    final MetaQualifier[] metaQualifiers = HostWithSimpleMetaQualifiedQualifier.class.getAnnotationsByType(MetaQualifier.class);
    assertNotNull(metaQualifiers);
    assertEquals(0, metaQualifiers.length);    
  }

  @Test
  public void testInheritedMetaQualifierAbsent() throws Exception {
    final InheritedMetaQualifier[] metaQualifiers = HostWithSimpleInheritedMetaQualifiedQualifier.class.getAnnotationsByType(InheritedMetaQualifier.class);
    assertNotNull(metaQualifiers);
    assertEquals(0, metaQualifiers.length);    
  }

  @Test
  public void testIsUltimatelyQualifiedWith1() throws Exception {
    final Annotation[] annotations = getAnnotationsQualifiedWith(HostWithSimpleQualifier.class, SimpleQualifier.class);
    assertNotNull(annotations);
    assertEquals(1, annotations.length);
  }

  @Test
  public void testIsUltimatelyQualifiedWith2() throws Exception {
    Annotation[] annotations = getAnnotationsQualifiedWith(HostWithSimpleMetaQualifiedQualifier.class, MetaQualifier.class);
    assertNotNull(annotations);
    assertEquals(1, annotations.length);
  }

  private static final boolean isBlacklisted(final Annotation annotation) {
    return annotation == null || isBlacklisted(annotation.annotationType());
  }

  private static final boolean isBlacklisted(final Class<? extends Annotation> c) {
    final boolean returnValue;
    if (c == null) {
      returnValue = true;
    } else {
      final String className = c.getName();
      returnValue = className.startsWith("java.") || className.startsWith("javax.");
    }
    return returnValue;
  }

  private static final Annotation[] getAnnotationsQualifiedWith(final Annotated host, final Class<? extends Annotation> metaAnnotationType, final BeanManager beanManager) {
    return getAnnotationsQualifiedWith(host.getAnnotations(), metaAnnotationType, beanManager);
  }
  
  private static final Annotation[] getAnnotationsQualifiedWith(final AnnotatedElement host, final Class<? extends Annotation> metaAnnotationType) {
    return getAnnotationsQualifiedWith(Arrays.asList(host.getAnnotations()), metaAnnotationType, null);
  }

  private static final Annotation[] getAnnotationsQualifiedWith(final Collection<? extends Annotation> suppliedAnnotations, final Class<? extends Annotation> metaAnnotationType, final BeanManager beanManager) {
    Objects.requireNonNull(suppliedAnnotations);
    Objects.requireNonNull(metaAnnotationType);
    final Collection<Annotation> results = new LinkedHashSet<>();
    final Deque<Annotation> annotationsToProcess = new ArrayDeque<>(suppliedAnnotations);
    final Set<Annotation> processedAnnotations = new HashSet<Annotation>();
    while (!annotationsToProcess.isEmpty()) {
      final Annotation annotation = annotationsToProcess.removeFirst();
      assert annotation != null;
      if (!isBlacklisted(annotation)) {
        processedAnnotations.add(annotation);
        
        final Class<? extends Annotation> annotationType = annotation.annotationType();
        assert annotationType != null;        
        
        if (annotationType.equals(metaAnnotationType)) {
          results.add(annotation);
        } else {

          // TODO: convert/lookup corresponding Annotated representing this annotationType
          
          final Collection<? extends Annotation> metaAnnotations = getAnnotations(annotationType, beanManager);
          if (metaAnnotations != null && !metaAnnotations.isEmpty()) {
            for (final Annotation metaAnnotation : metaAnnotations) {
              assert metaAnnotation != null;
              if (!isBlacklisted(metaAnnotation) && !processedAnnotations.contains(metaAnnotation)) {
                annotationsToProcess.addLast(metaAnnotation);
              }
            }
          }
        }
        
      }
    }
    processedAnnotations.clear();
    final Annotation[] returnValue = results.toArray(new Annotation[results.size()]);
    return returnValue;
  }

  private static final <X> Collection<? extends Annotation> getAnnotations(final Class<X> c, final BeanManager beanManager) {
    Objects.requireNonNull(c);
    final Collection<? extends Annotation> returnValue;
    if (beanManager != null) {
      final Annotated annotated = beanManager.createAnnotatedType(c);
      assert annotated != null;
      returnValue = annotated.getAnnotations();
    } else {
      returnValue = Arrays.asList(c.getAnnotations());
    }
    return returnValue;
  }

  @Documented
  @MetaQualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE })
  private @interface SimpleMetaQualifiedQualifier {

  }

  @Documented
  @InheritedMetaQualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE })
  private @interface SimpleInheritedMetaQualifiedQualifier {

  }

  @Documented
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE })
  private @interface SimpleQualifier {

  }

  @Documented
  @Inherited
  @Qualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.ANNOTATION_TYPE })
  private @interface InheritedMetaQualifier {

  }

  @Documented
  @Qualifier
  @Retention(value = RetentionPolicy.RUNTIME)
  @Target({ ElementType.ANNOTATION_TYPE })
  private @interface MetaQualifier {

  }

  @SimpleQualifier
  private static final class HostWithSimpleQualifier {

  }

  @SimpleMetaQualifiedQualifier
  private static final class HostWithSimpleMetaQualifiedQualifier {

  }

  @SimpleInheritedMetaQualifiedQualifier
  private static final class HostWithSimpleInheritedMetaQualifiedQualifier {

  }
  
}
