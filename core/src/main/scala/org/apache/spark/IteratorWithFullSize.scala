package org.apache.spark

/**
  * This class wraps an iterator and returns number of processed elements instead of number of elements left
  */
private[this] class IteratorWithFullSize[T](it: Iterator[T]) extends Iterator[T]{
  private var processedElements = 0

  override def hasNext: Boolean = it.hasNext

  override def next(): T = {
    processedElements = processedElements+1
    it.next()
  }

  override def size: Int = processedElements
}

object IteratorWithFullSize{
  def apply[T](it: Iterator[T]) = new IteratorWithFullSize[T](it)
}