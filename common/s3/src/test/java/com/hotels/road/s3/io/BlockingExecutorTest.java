/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.s3.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import lombok.RequiredArgsConstructor;

@RunWith(MockitoJUnitRunner.class)
public class BlockingExecutorTest {

  @Mock
  private ExecutorService delegate;
  @Mock
  private Semaphore semaphore;
  @Mock
  private Runnable runnable;
  @Captor
  ArgumentCaptor<Runnable> captor;

  private BlockingExecutor underTest;

  @Test
  public void executeHappyPath() throws InterruptedException {
    underTest = new BlockingExecutor(delegate, semaphore);

    underTest.execute(runnable);

    InOrder inOrder = inOrder(delegate, semaphore);
    inOrder.verify(semaphore).acquire();
    inOrder.verify(delegate).execute(captor.capture());
    inOrder.verify(semaphore, never()).release();

    captor.getValue().run();

    InOrder inOrder2 = inOrder(runnable, semaphore);
    inOrder2.verify(runnable).run();
    inOrder2.verify(semaphore).release();

  }

  @Test
  public void delegateException() throws InterruptedException {
    doThrow(RuntimeException.class).when(delegate).execute(any(Runnable.class));

    underTest = new BlockingExecutor(delegate, semaphore);

    try {
      underTest.execute(runnable);
      fail();
    } catch (RuntimeException e) {
      InOrder inOrder = inOrder(delegate, semaphore);
      inOrder.verify(semaphore).acquire();
      inOrder.verify(delegate).execute(any(Runnable.class));
      inOrder.verify(semaphore).release();
    }
  }

  @Test
  public void runnableException() throws InterruptedException {
    doThrow(RuntimeException.class).when(runnable).run();

    underTest = new BlockingExecutor(delegate, semaphore);

    underTest.execute(runnable);
    InOrder inOrder = inOrder(delegate, semaphore);
    inOrder.verify(semaphore).acquire();
    inOrder.verify(delegate).execute(captor.capture());
    inOrder.verify(semaphore, never()).release();

    try {
      captor.getValue().run();
      fail();
    } catch (RuntimeException e) {
      InOrder inOrder2 = inOrder(runnable, semaphore);
      inOrder2.verify(runnable).run();
      inOrder2.verify(semaphore).release();
    }
  }

  @Test(expected = RuntimeException.class)
  public void semaphoreInterruptedException() throws InterruptedException {
    underTest = new BlockingExecutor(delegate, semaphore);

    doThrow(InterruptedException.class).when(semaphore).acquire();

    underTest.execute(runnable);
  }

  @Test(timeout = 5000L)
  public void x() throws InterruptedException {
    BlockingExecutor underTest = new BlockingExecutor(1, 1);

    TestRunnable r1 = new TestRunnable("r1");
    TestRunnable r2 = new TestRunnable("r2");
    TestRunnable r3 = new TestRunnable("r3");

    TestThread t1 = new TestThread(r1, underTest);
    TestThread t2 = new TestThread(r2, underTest);
    TestThread t3 = new TestThread(r3, underTest);

    t1.start();
    Thread.sleep(100L);
    t2.start();
    Thread.sleep(100L);
    t3.start();
    Thread.sleep(100L);

    assertThat(t1.state, is(ThreadState.RUNNING));
    assertThat(t2.state, is(ThreadState.RUNNING));
    assertThat(t3.state, is(ThreadState.BLOCKED));
    assertThat(r1.state, is(RunnableState.RUNNING));
    assertThat(r2.state, is(RunnableState.WAITING)); // in queue
    assertThat(r3.state, is(RunnableState.WAITING)); // blocked

    r1.finish();
    Thread.sleep(100L);
    t1.join();

    assertThat(t3.state, is(ThreadState.RUNNING));
    assertThat(r1.state, is(RunnableState.FINISHED));
    assertThat(r2.state, is(RunnableState.RUNNING));
    assertThat(r3.state, is(RunnableState.WAITING)); // in queue

    r2.finish();
    Thread.sleep(100L);
    t2.join();

    assertThat(r2.state, is(RunnableState.FINISHED));
    assertThat(r3.state, is(RunnableState.RUNNING));

    r3.finish();
    Thread.sleep(100L);
    t3.join();

    assertThat(r3.state, is(RunnableState.FINISHED));
  }

  @RequiredArgsConstructor
  static class TestThread extends Thread {
    private final TestRunnable runnable;
    private final Executor executor;
    private ThreadState state = ThreadState.BLOCKED;

    @Override
    public void run() {
      executor.execute(runnable);
      state = ThreadState.RUNNING;
    }
  }

  @RequiredArgsConstructor
  static class TestRunnable implements Runnable {
    private final String name;
    private RunnableState state = RunnableState.WAITING;
    private boolean finish = false;

    @Override
    public void run() {
      state = RunnableState.RUNNING;
      while (!finish) {
        try {
          Thread.sleep(10L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      state = RunnableState.FINISHED;
    }

    void finish() {
      finish = true;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  enum ThreadState {
    BLOCKED,
    RUNNING;
  }

  enum RunnableState {
    WAITING,
    RUNNING,
    FINISHED;
  }

}
