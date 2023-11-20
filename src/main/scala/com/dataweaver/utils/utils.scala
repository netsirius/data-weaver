package com.dataweaver.utils

object Utils {

  /**
   * Retrieves a value from a configuration map based on the given key.
   *
   * This method attempts to fetch a value associated with a key from the provided configuration map.
   * If the key is not found, it throws an IllegalArgumentException. This helps in ensuring that the
   * application configuration is correctly provided before proceeding with further operations.
   *
   * @param config The configuration map from which to fetch the value.
   * @param key    The key for which the value is to be fetched.
   * @return The value associated with the specified key.
   * @throws IllegalArgumentException if the key is not found in the configuration map.
   */
  def getConfigValue(config: Map[String, String], key: String): String = {
    config.getOrElse(key, throw new IllegalArgumentException(s"Key not found in config: $key"))
  }
}