package com.dataweaver.config

/**
 * Represents the configuration for a data transformation.
 *
 * @param id       The unique identifier for the transformation.
 * @param `type`   The type of the transformation.
 * @param sources  The list of source identifiers for this transformation.
 * @param targetId The identifier for the target data source of this transformation.
 * @param query    The transformation query.
 */
case class TransformationConfig(id: String, `type`: String, sources: List[String], targetId: String, query: String)