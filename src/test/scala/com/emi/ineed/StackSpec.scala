package com.emi.ineed

import collection.mutable.Stack
import org.scalatest._

class StackSpec extends FunSuite {

  test ( "Init test with a stack in order to have a unit test" ) {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }
}
