package org.apache.bigds.repl

object Main {
  private var _interp: BigdsILoop = _

  def interp = _interp

  def interp_=(i: BigdsILoop) { _interp = i }

  def main(args: Array[String]) {
    _interp = new BigdsILoop
    _interp.process(args)
  }
}
