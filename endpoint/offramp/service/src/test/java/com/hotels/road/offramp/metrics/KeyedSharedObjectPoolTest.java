package com.hotels.road.offramp.metrics;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.Mockito;

public class KeyedSharedObjectPoolTest {
  @SuppressWarnings("unchecked")
  KeyedSharedObjectPool<Integer, Object> underTest = Mockito.spy(KeyedSharedObjectPool.class);

  @Test
  public void object_is_destroyed_when_returned() throws Exception {
    Integer key = 1;
    Object value = new Object();

    when(underTest.constructValue(key)).thenReturn(value);

    assertThat(underTest.take(key), is(theInstance(value)));

    underTest.release(key);

    verify(underTest).destroyValue(key, value);
  }

  @Test
  public void same_object_is_returned_for_the_same_key() throws Exception {
    Integer key = 1;
    Object value = new Object();

    when(underTest.constructValue(key)).thenReturn(value);

    assertThat(underTest.take(key), is(theInstance(value)));
    assertThat(underTest.take(key), is(theInstance(value)));
  }

  @Test
  public void object_is_destroyed_only_when_all_returned() throws Exception {
    Integer key = 1;
    Object value = new Object();

    when(underTest.constructValue(key)).thenReturn(value);

    assertThat(underTest.take(key), is(theInstance(value)));
    assertThat(underTest.take(key), is(theInstance(value)));

    underTest.release(key);

    verify(underTest, times(0)).destroyValue(key, value);

    underTest.release(key);

    verify(underTest).destroyValue(key, value);
  }

  @Test
  public void different_keys_construct_different_objects() throws Exception {
    Object value1 = new Object();
    Object value2 = new Object();

    when(underTest.constructValue(1)).thenReturn(value1);
    when(underTest.constructValue(2)).thenReturn(value2);

    assertThat(underTest.take(1), is(theInstance(value1)));
    assertThat(underTest.take(2), is(theInstance(value2)));
  }
}
