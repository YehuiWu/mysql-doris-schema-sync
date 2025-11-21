package com.qingmei.schema.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TypeMapperTest {
  @Test
  void varcharTriplesLength() {
    String t = TypeMapper.toDorisType("varchar", 10L, null, null);
    assertEquals("varchar(30)", t);
  }

  @Test
  void bitToInt() {
    String t = TypeMapper.toDorisType("bit", null, null, null);
    assertEquals("int", t);
  }

  @Test
  void textToString() {
    String t = TypeMapper.toDorisType("text", null, null, null);
    assertEquals("string", t);
  }
}