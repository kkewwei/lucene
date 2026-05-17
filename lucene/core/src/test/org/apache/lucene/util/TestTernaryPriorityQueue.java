/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTernaryPriorityQueue extends LuceneTestCase {

  private static class IntegerQueue extends TernaryPriorityQueue<Integer> {
    public IntegerQueue(int count) {
      super(count, (a, b) -> a < b);
    }

    protected final void checkValidity() {
      Object[] heapArray = getHeapArray();
      int size = size();
      for (int parent = 1; parent <= size; parent++) {
        int firstChild = ARITY * (parent - 1) + 2;
        int lastChild = Math.min(firstChild + ARITY - 1, size);
        for (int c = firstChild; c <= lastChild; c++) {
          assertTrue((Integer) heapArray[parent] < (Integer) heapArray[c]);
        }
      }
    }
  }

  public void testTPQ() {
    int size = atLeast(10000);
    TernaryPriorityQueue<Integer> pq = new IntegerQueue(size);
    Random gen = random();

    int sum = 0, sum2 = 0;

    for (int i = 0; i < size; i++) {
      int next = gen.nextInt();
      sum += next;
      pq.add(next);
    }

    int last = Integer.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      Integer next = pq.pop();
      assertTrue(next > last);
      last = next.intValue();
      sum2 += last;
    }

    assertEquals(sum, sum2);
  }

  public void testClear() {
    TernaryPriorityQueue<Integer> pq = new IntegerQueue(3);
    pq.add(2);
    pq.add(3);
    pq.add(1);
    assertEquals(3, pq.size());
    pq.clear();
    assertEquals(0, pq.size());
  }

  public void testFixedSize() {
    TernaryPriorityQueue<Integer> pq = new IntegerQueue(3);
    pq.insertWithOverflow(2);
    pq.insertWithOverflow(3);
    pq.insertWithOverflow(1);
    pq.insertWithOverflow(5);
    pq.insertWithOverflow(7);
    pq.insertWithOverflow(1);
    assertEquals(3, pq.size());
    assertEquals((Integer) 3, pq.top());
  }

  public void testInsertWithOverflow() {
    int size = 4;
    IntegerQueue pq = new IntegerQueue(size);
    Integer i1 = 2;
    Integer i2 = 3;
    Integer i3 = 1;
    Integer i4 = 5;
    Integer i5 = 7;
    Integer i6 = 1;

    assertNull(pq.insertWithOverflow(i1));
    assertNull(pq.insertWithOverflow(i2));
    assertNull(pq.insertWithOverflow(i3));
    assertNull(pq.insertWithOverflow(i4));
    assertEquals(i3, pq.insertWithOverflow(i5));
    assertEquals(i6, pq.insertWithOverflow(i6));
    assertEquals(size, pq.size());
    assertEquals(2, (int) pq.top());
  }

  public void testAddAll() {
    IntegerQueue pq = new IntegerQueue(20);
    List<Integer> originValues = new ArrayList<>();
    Random random = random();

    for (int i = 0; i < 10; i++) {
      int x = random.nextInt();
      pq.add(x);
      originValues.add(x);
    }

    List<Integer> bulkAdded = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      int x = random.nextInt();
      bulkAdded.add(x);
      originValues.add(x);
    }

    pq.addAll(bulkAdded);
    pq.checkValidity();

    Collections.sort(originValues);

    for (int i = 0; i < pq.size; i++) {
      assertEquals(originValues.get(i), pq.pop());
    }
    assertEquals(0, pq.size());

    originValues.add(random.nextInt());

    expectThrows(ArrayIndexOutOfBoundsException.class, () -> pq.addAll(originValues));
  }

  public void testUpdateTop() {
    IntegerQueue pq = new IntegerQueue(5);
    pq.add(5);
    pq.add(3);
    pq.add(8);
    pq.add(1);
    pq.add(4);
    assertEquals((Integer) 1, pq.top());

    assertEquals((Integer) 3, pq.updateTop(10));
    pq.checkValidity();

    assertEquals((Integer) 3, pq.updateTop(3));
    pq.checkValidity();

    int last = Integer.MIN_VALUE;
    while (pq.size() > 0) {
      Integer next = pq.pop();
      assertTrue(next > last);
      last = next;
    }
  }

  public void testDrainToArrayLowestFirst() {
    IntegerQueue pq = new IntegerQueue(5);
    pq.add(3);
    pq.add(1);
    pq.add(4);
    pq.add(1);
    pq.add(5);

    Integer[] arr = pq.drainToArrayLowestFirst(Integer[]::new);
    assertArrayEquals(new Integer[] {1, 1, 3, 4, 5}, arr);
    assertEquals(0, pq.size());
  }

  public void testDrainToArrayHighestFirst() {
    IntegerQueue pq = new IntegerQueue(5);
    pq.add(3);
    pq.add(1);
    pq.add(4);
    pq.add(1);
    pq.add(5);

    Integer[] arr = pq.drainToArrayHighestFirst(Integer[]::new);
    assertArrayEquals(new Integer[] {1, 1, 3, 4, 5}, arr);
    assertEquals(0, pq.size());
  }

  public void testInvalid() {
    expectThrows(IllegalArgumentException.class, () -> new IntegerQueue(-1));
    expectThrows(
        IllegalArgumentException.class, () -> new IntegerQueue(ArrayUtil.MAX_ARRAY_LENGTH));
  }
}
