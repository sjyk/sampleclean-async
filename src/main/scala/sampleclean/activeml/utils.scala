package sampleclean.activeml

import java.util.UUID

/**
  * Utilities for the activeml package.
  */
object utils {

  /** Generate a random uuid */
  def randomUUID(): String = {
    UUID.randomUUID.toString
  }
}
