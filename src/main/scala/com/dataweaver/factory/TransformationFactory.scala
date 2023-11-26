package com.dataweaver.factory

import com.dataweaver.config.TransformationConfig
import com.dataweaver.transformation.{SQLTransformation, Transformation}

/** Factory for creating transformation instances based on the provided configuration.
  */
object TransformationFactory {

  /** Creates a transformation instance based on the provided configuration.
    *
    * @param config
    *   The configuration of the transformation.
    * @return
    *   A transformation instance.
    * @throws IllegalArgumentException
    *   If the transformation type is not supported.
    */
  def create(config: TransformationConfig): Transformation = {
    config.`type` match {
      case "SQLTransformation" => new SQLTransformation(config)
      case _ => throw new IllegalArgumentException("Transformation type is not supported")
    }
  }
}
