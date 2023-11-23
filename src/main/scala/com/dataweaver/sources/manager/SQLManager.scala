package com.dataweaver.sources.manager

import java.sql.{Connection, DriverManager}

class SQLManager extends DataSourceManager with Serializable {
  def getConnection(url: String, user: String, password: String): Connection = {
    DriverManager.getConnection(url, user, password)
  }
}
