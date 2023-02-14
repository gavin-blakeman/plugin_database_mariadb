#include "include/database_mariadb.h"

  // engineeringShop

#include "include/database/database/record.h"
#include "include/database/database/databaseVariant.h"

//#define DEBUG_ON
#undef DEBUG_ON

namespace database
{

  /// @brief Constructor for the connectors.
  /// @param[in] poolSize: The size of the connection pool.
  /// @version 2022-09-28/GGB - Function created.

  CMariaDBConnector::CMariaDBConnector(handle_t poolSize) : CConnectionPool(poolSize), connectionPool(poolSize)
  {
    for (handle_t i = 0; i < poolSize; i++)
    {
      connectionPool[i].mysql = mysql_init(nullptr);
      connectionPool[i].mysql_res = nullptr;
      connectionPool[i].mysql_field = nullptr;
      connectionPool[i].v = 0;
    }
  }

  /// @brief Destructor - Ensure all open connectoions are closed.
  /// @version 2022-09-28/GGB - Function created.

  CMariaDBConnector::~CMariaDBConnector()
  {
    for (auto &connection :  connectionPool)
    {
      mysql_close(connection.mysql);
      connection.mysql = nullptr;
      connection.mysql_res = nullptr;
      connection.mysql_field = nullptr;
    }
  }

  /// @brief Create and populate the struct for the bind parameters before calling the prepared statement.
  /// @param[in] handle: The connection pool handle.
  /// @throws
  /// @version 2022-10-25/GGB - Function created.


  void CMariaDBConnector::createInputParameters(handle_t handle)
  {
    connectionPool[handle].mysql_bind = std::make_unique<MYSQL_BIND[]>(connectionPool[handle].inputParameters.size());

    std::size_t index = 0;
    for (auto &bv: connectionPool[handle].inputParameters)
    {
      enum_field_types fieldType;

      switch(bv.type())
      {
        case BIT:
        {
          fieldType = MYSQL_TYPE_BIT;

          break;
        }
        case BLOB:
        {
          fieldType = MYSQL_TYPE_BLOB;
          connectionPool[handle].mysql_bind[index].buffer = 0;
          break;
        }
        case U8:
        {
          fieldType = MYSQL_TYPE_TINY;
          connectionPool[handle].mysql_bind[index].is_unsigned = true;
          break;
        };
        case I8:
        {
          fieldType = MYSQL_TYPE_TINY;
          connectionPool[handle].mysql_bind[index].buffer_length = sizeof(std::int8_t);
          break;
        };
        case U16:
        {
          fieldType = MYSQL_TYPE_SHORT;
          connectionPool[handle].mysql_bind[index].is_unsigned = true;
          connectionPool[handle].mysql_bind[index].buffer_length = sizeof(std::uint16_t);
          break;
        }
        case I16:
        {
          fieldType = MYSQL_TYPE_SHORT;
          connectionPool[handle].mysql_bind[index].buffer_length = sizeof(std::int16_t);
          break;
        }
        case U32:
        {
          fieldType = MYSQL_TYPE_LONG;
          connectionPool[handle].mysql_bind[index].is_unsigned = true;
          connectionPool[handle].mysql_bind[index].buffer_length = sizeof(std::uint32_t);
          break;
        }
        case I32:
        {
          fieldType = MYSQL_TYPE_LONG;
          connectionPool[handle].mysql_bind[index].buffer_length = sizeof(std::int32_t);
          break;
        }
        case U64:
        {
          fieldType = MYSQL_TYPE_LONGLONG;
          connectionPool[handle].mysql_bind[index].is_unsigned = true;
          break;
        }
        case I64:
        {
          fieldType = MYSQL_TYPE_LONGLONG;
          break;
        }
        case FLOAT:
        {
          fieldType = MYSQL_TYPE_FLOAT;
          break;
        }
        case DOUBLE:
        {
          fieldType = MYSQL_TYPE_DOUBLE;
          break;
        }
        case STRING:
        {
          fieldType = MYSQL_TYPE_VAR_STRING;
          break;
        }
        case NULLVALUE:
        {
          fieldType = MYSQL_TYPE_NULL;
          break;
        }
        case BOOL:
        {
          fieldType = MYSQL_TYPE_BOOL;
          break;
        }
        case DATE:
        {
          fieldType = MYSQL_TYPE_DATE;
          break;
        }
        case TIME:
        {
          fieldType = MYSQL_TYPE_TIME;
          break;
        }
        case DATETIME:
        {
          fieldType = MYSQL_TYPE_DATETIME;
          break;
        }
        case DECIMAL:
        {
          fieldType = MYSQL_TYPE_DECIMAL;
          break;
        }
        default:
        {
          CODE_ERROR();
        }
      };

      connectionPool[handle].mysql_bind[index].buffer_type = fieldType;
      connectionPool[handle].mysql_bind[index].buffer = static_cast<void *>(bv);
      connectionPool[handle].mysql_bind[index].buffer_length = bv.bufferLength();
    }
  }

  /// @brief Factory function.
  /// @param[in] poolSize: The size of the pool.
  /// @param[in] cr: Configuration reader.
  /// @throws
  /// @version 2022-09-27/GGB - Function created.

  CConnectionPool *CMariaDBConnector::createDatabaseConnector(database::handle_t poolSize, GCL::CReaderSections *cr)
  {
    return new CMariaDBConnector(poolSize);
  }

  /// @brief Loads the row data for the current row.
  /// @param[in] handle: The handle to load.
  /// @throws
  /// @version 2022-09-20/GGB - Function created.

  void CMariaDBConnector::loadRow(handle_t handle)
  {
    if (connectionPool[handle].rowCursorActual != connectionPool[handle].rowCursorRequested)
    {
      mysql_data_seek(connectionPool[handle].mysql_res, connectionPool[handle].rowCursorRequested);
      connectionPool[handle].rowCursorActual = connectionPool[handle].rowCursorRequested;
    };

    connectionPool[handle].mysql_row = mysql_fetch_row(connectionPool[handle].mysql_res);
    connectionPool[handle].rowCursorActual++;       // mysql_fetch_row moves to next row.
    connectionPool[handle].columnLengths = mysql_fetch_lengths(connectionPool[handle].mysql_res);

    connectionPool[handle].validRecord = true;
  }

  /// @brief Adds a positional binding value.
  /// @param[in] handle: The connection handle in use.
  /// @param[in] v: The value to bind.
  /// @param[in] pt: The type of parameter.
  /// @throws
  /// @version 2022-10-20/GGB - Function created.

  void CMariaDBConnector::processAddBindValue(handle_t handle, CVariant const &bindValue)
  {
    if ( (bindValue.paramType() == PT_IN) || (bindValue.paramType() == PT_INOUT) )
    {
      connectionPool[handle].inputParameters.push_back(bindValue);
    };

    if ( (bindValue.paramType() == PT_OUT) || (bindValue.paramType() == PT_INOUT) )
    {
      connectionPool[handle].outputParameters.push_back(bindValue);
    }
  }


  /// @brief      Begins a transaction. Starts by opening the connection if required and then sending a "START TRANSACTION" to the
  ///             server.
  /// @param[in]  handle: The connection handle to use.
  /// @throws
  /// @version    2022-09-28/GGB - Function created.

  void CMariaDBConnector::processBeginTransaction(handle_t handle)
  {
    std::string const STARTTRANSACTION = "START TRANSACTION";

      // Create the 'real' connection if not already created.

    if (!connectionPool[handle].connectedFlag)
    {
      if (mysql_real_connect(connectionPool[handle].mysql,
                             host_.c_str(),
                             user_.c_str(),
                             passwd_.c_str(),
                             schema_.c_str(),
                             port_,
                             "",
                             0))
      {
        connectionPool[handle].connectedFlag = true;
      }
      else
      {
          // Unable to connect.

        RUNTIME_ERROR(processError(handle));
      }
    }

    DEBUGMESSAGE(STARTTRANSACTION);

    if (!mysql_real_query(connectionPool[handle].mysql, STARTTRANSACTION.c_str(), STARTTRANSACTION.length()))
    {
      connectionPool[handle].tip = true;
    }
    else
    {
        // Error.

      RUNTIME_ERROR(processError(handle));
    }
  }

  /// @brief      Commits the current transaction.
  /// @param[in]  handle: The connection handle
  /// @throws
  /// @version    2022-09-28/GGB - Function created.

  void CMariaDBConnector::processCommitTransaction(handle_t handle)
  {
    std::string const COMMITTRANSACTION = "COMMIT";

    DEBUGMESSAGE("Committing last transaction");
    if (mysql_real_query(connectionPool[handle].mysql, COMMITTRANSACTION.c_str(), COMMITTRANSACTION.length()))
    {
      RUNTIME_ERROR(processError(handle));
    }

    if (connectionPool[handle].mysql_res)
    {
      mysql_free_result(connectionPool[handle].mysql_res);
      connectionPool[handle].mysql_res = nullptr;
      connectionPool[handle].tip = false;
    };

    DEBUGMESSAGE(COMMITTRANSACTION);

    connectionPool[handle].validRecord = false;
  }

  /// @brief Ends a transaction.
  /// @param[in] handle: The connection handle.
  /// @throws
  /// @version 2022-10-29/GGB - Function created.

  void CMariaDBConnector::processEndTransaction(handle_t handle)
  {
    if (connectionPool[handle].tip)
    {
      processCommitTransaction(handle);
      connectionPool[handle].tip = false;
    }
  }

  /// @brief Processes an error, by loading the error number and code.
  /// @returns The error number and error code.
  /// @version 2022-09-28/GGB - Function created.

  std::string CMariaDBConnector::processError(handle_t handle)
  {
    unsigned int errorNo = mysql_errno(connectionPool[handle].mysql);
    std::string errorText = mysql_error(connectionPool[handle].mysql);

    return std::to_string(errorNo) + " - " + errorText;
  }

  /// @brief Executes a prepared statement. The statement is prepared, variables assigned and executed in this function.
  /// @throws
  /// @version 2022-10-20/GGB - Function created.

  void CMariaDBConnector::processExec(handle_t handle)
  {
    if (!connectionPool[handle].prepareStatement)
    {
      RUNTIME_ERROR("No statement prepared.");
    }

    connectionPool[handle].mysql_stmt = mysql_stmt_init(connectionPool[handle].mysql);

    if (mysql_stmt_prepare(connectionPool[handle].mysql_stmt,
                           connectionPool[handle].preparedStatement.c_str(),
                           connectionPool[handle].preparedStatement.length()))
    {
      RUNTIME_ERROR(processError(handle));
    }

    createInputParameters(handle);

    if (!mysql_stmt_bind_param(connectionPool[handle].mysql_stmt,
                               connectionPool[handle].mysql_bind.get()))
    {
      RUNTIME_ERROR(processError(handle));
    }

    if (mysql_stmt_execute(connectionPool[handle].mysql_stmt))
    {
      RUNTIME_ERROR(processError(handle));
    }

    if (!connectionPool[handle].outputParameters.empty())
    {
      if ((connectionPool[handle].mysql_res = mysql_stmt_result_metadata(connectionPool[handle].mysql_stmt)))
      {
          // Process the results from the query.


      }
      else
      {
        RUNTIME_ERROR("Output Parameters specified, but query does/did not produce a result.");
      }
    }

  }

  /// @brief      Gets a record from the last query.
  /// @param[in]  handle: The connection to utilise.
  /// @returns    A CRecord containing the information.
  /// @version    2022-09-28/GGB - Function created.

  void CMariaDBConnector::processGetRecord(handle_t handle, ::database::CRecord &record)
  {
    record.clear();

      // Row is loaded by the move functions.

    if (connectionPool[handle].validRecord)
    {
      for (std::size_t columnIndex = 0; columnIndex < connectionPool[handle].columnCount; ++columnIndex)
      {
        record.setValue(columnIndex, processColumnValue(handle, columnIndex));
      }
    }
    else
    {
      RUNTIME_ERROR("Record not loaded.");
    }

#ifdef DEBUG_ON
    DEBUGMESSAGE("End processGetRecord");
#endif
  }

  /// @brief      Retrieves an entire recorset from the database. This is a read-only recordSet.
  /// @param[in]  handle: The connection to utilise.
  /// @returns    A CRecord containing the information.
  /// @version    2022-09-28/GGB - Function created.

  void CMariaDBConnector::processGetRecordSet(handle_t handle, ::database::CRecordSet &recordSet)
  {
    recordSet.clear();
    recordSet.resize(connectionPool[handle].rowCount);

    std::size_t recordIndex = 0;

#ifdef DEBUG_ON
    DEBUGMESSAGE("ProcessGetRecordSet");
#endif

    if (moveFirst(handle))
    {
      do
      {
        processGetRecord(handle, recordSet[recordIndex++]);
      }
      while (moveNext(handle));
    }
  }

  /// @brief      Moves the rowCursor to the next row and loads the data.
  /// @param[in]  handle: The connectionPool handle.
  /// @throws
  /// @version    2022-09-20/GGB - Function created.

  bool CMariaDBConnector::processMoveFirst(handle_t handle)
  {
    bool returnValue = false;

    if (connectionPool[handle].rowCount > 0)
    {
      connectionPool[handle].rowCursorRequested = 0;
      loadRow(handle);
      returnValue = true;
    }

    return returnValue;
  }

  bool CMariaDBConnector::processMoveNext(handle_t handle)
  {
    bool returnValue = false;

    connectionPool[handle].rowCursorRequested++;

    if (connectionPool[handle].rowCount > connectionPool[handle].rowCursorRequested)
    {
      loadRow(handle);
      returnValue = true;
    }
    else
    {
      connectionPool[handle].rowCursorRequested--;
    }

    return returnValue;
  }

  bool CMariaDBConnector::processMovePrevious(handle_t handle)
  {
    bool returnValue = false;

    if (connectionPool[handle].rowCursorRequested != 0)
    {
      connectionPool[handle].rowCursorActual--;
      loadRow(handle);
      returnValue = true;
    };

    return returnValue;
  }

  /// @brief Prepares a prepared statement.
  /// @param[in] handle: The connection pool handle.
  /// @param[in] sqlQuery: The query containing the binding placeholders.
  /// @returns true if no errors.
  /// @throws
  /// @version 2022-10-20/GGB - Function created.

  bool CMariaDBConnector::processPrepareQuery(handle_t handle, std::string const &sqlQuery)
  {
    connectionPool[handle].prepareStatement = true;

  }


  /// @brief Process a query and stores the number of fields returned.
  /// @param[in] handle: The connection pool handle.
  /// @param[in] query: The query to execute.
  /// @throws
  /// @version 2022-09-20/GGB - Function created.

  void CMariaDBConnector::processQuery(handle_t handle, std::string const &query)
  {
    DEBUGMESSAGE(query);
    if (!mysql_real_query(connectionPool[handle].mysql, query.c_str(), query.length()))
    {
      connectionPool[handle].columnCount = mysql_field_count(connectionPool[handle].mysql);

      if (connectionPool[handle].columnCount != 0)
      {
        processResults(handle); // This is needed to prevent the next connection failing.
        connectionPool[handle].rowCursorActual = 0;
        connectionPool[handle].rowCursorRequested = 0;
        if (connectionPool[handle].rowCount != 0)
        {
          loadRow(handle);
        };
      };
    }
    else
    {
      RUNTIME_ERROR(processError(handle));
    };
  }

  /// @brief Function called after a query that returns results. Fetches the results.
  /// @param[in] handle:
  /// @throws
  /// @version 2022-09-20/GGB - Function created.

  void CMariaDBConnector::processResults(handle_t handle)
  {
      // Check if the result is available and if not, try to load it.

    connectionPool[handle].mysql_res = mysql_store_result(connectionPool[handle].mysql);

      // Result available?

    if (!connectionPool[handle].mysql_res)
    {
      RUNTIME_ERROR("Unable to retrieve query results.");
    }
    else
    {
      connectionPool[handle].rowCount = mysql_num_rows(connectionPool[handle].mysql_res);

#ifdef DEBUG_ON
      DEBUGMESSAGE("Row Count: " + std::to_string(connectionPool[handle].rowCount));
#endif
      connectionPool[handle].mysql_field = mysql_fetch_fields(connectionPool[handle].mysql_res);
    }
  }

  /// @brief Rolls back the current transaction.
  /// @param[in] handle: The connectionPool handle.
  /// @throws
  /// @version 2022-10-29/GGB - Function created.

  void CMariaDBConnector::processRollbackTransaction(handle_t handle)
  {
    if (!mysql_rollback(connectionPool[handle].mysql))
    {
      connectionPool[handle].tip = false;
    }
    else
    {
      RUNTIME_ERROR(processError(handle));
    };

    DEBUGMESSAGE("ROLLBACK TRANSACTION");
  }


  /// @brief Processes a column value.
  /// @param[in] handle: Handle for the connection
  /// @param[in] columnIndex: The index of the column to access.
  /// @returns A CVariant containing the value.
  /// @throws
  /// @version 2022-09-29/GGB - Function created.

  ::database::CVariant CMariaDBConnector::processColumnValue(handle_t handle, std::size_t columnIndex)
  {
    ::database::CVariant returnValue;

    bool unsignedValue = false;

    char *columnValue = connectionPool[handle].mysql_row[columnIndex];
    unsigned long columnLength = connectionPool[handle].columnLengths[columnIndex];

    unsignedValue = connectionPool[handle].mysql_field[columnIndex].flags & UNSIGNED_FLAG;
    std::string columnName(connectionPool[handle].mysql_field[columnIndex].name,
                          connectionPool[handle].mysql_field[columnIndex].name_length);

#ifdef DEBUG_ON
    DEBUGMESSAGE("--- Start Column ---");
    DEBUGMESSAGE("Column Name: " + columnName);
    DEBUGMESSAGE("Column Index: " + std::to_string(columnIndex));
    DEBUGMESSAGE("Column Length: " + std::to_string(columnLength));
    DEBUGMESSAGE("Column unsigned: " + (unsignedValue ? std::string("Yes") : std::string("No")));
#endif


    std::string val(columnValue, columnLength);

#ifdef DEBUG_ON
    DEBUGMESSAGE("Value String: '" + val + "'");

    DEBUGMESSAGE("Field Type: " + std::to_string(connectionPool[handle].mysql_field[columnIndex].type));
#endif

    switch(connectionPool[handle].mysql_field[columnIndex].type)
    {
      case MYSQL_TYPE_DECIMAL:
      {
        DEBUGMESSAGE("Column Type: Decimal");
        break;
      }
      case MYSQL_TYPE_TINY:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: TINY");
#endif

        if (unsignedValue)
        {
          returnValue = static_cast<std::uint8_t>(std::stoul(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::uint8_t>(returnValue)));
#endif
        }
        else
        {
          returnValue = static_cast<std::int8_t>(std::stoi(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::int8_t>(returnValue)));
#endif
        }
        break;
      }
      case MYSQL_TYPE_SHORT:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: SHORT");
#endif

        if (unsignedValue)
        {
          returnValue = static_cast<std::uint16_t>(std::stoul(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::uint8_t>(returnValue)));
#endif
        }
        else
        {
          returnValue = static_cast<std::int16_t>(std::stoi(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::int8_t>(returnValue)));
#endif
        }
        break;
      }
      case MYSQL_TYPE_LONG:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: LONG");
#endif

        if (unsignedValue)
        {
          returnValue = static_cast<std::uint32_t>(std::stoul(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::uint32_t>(returnValue)));
#endif
        }
        else
        {
          returnValue = static_cast<std::int32_t>(std::stol(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::int32_t>(returnValue)));
#endif
        }
        break;
      }
      case MYSQL_TYPE_INT24:  // Cast to 32bit integer for use.
      {
        DEBUGMESSAGE("Column Type: INT24");

        if (unsignedValue)
        {
          //returnValue = *reinterpret_cast<std::uint32_t *>(columnValue);
        }
        else
        {
          //returnValue = *reinterpret_cast<std::int32_t *>(columnValue);
        }
        break;
      }
      case MYSQL_TYPE_FLOAT:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: Float");
#endif

        returnValue = std::stof(val);
        break;
      }
      case MYSQL_TYPE_DOUBLE:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: Double");
#endif

        returnValue = std::stod(val);
        break;
      }
      case MYSQL_TYPE_LONGLONG:
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: LONGLONG");
#endif

        std::string val(columnValue, columnLength);
        DEBUGMESSAGE("Value String: '" + val + "'");

        if (unsignedValue)
        {
          returnValue = static_cast<std::uint64_t>(std::stoull(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::uint64_t>(returnValue)));
#endif
        }
        else
        {
          returnValue = static_cast<std::int64_t>(std::stoll(val));
#ifdef DEBUG_ON
          DEBUGMESSAGE("Value: " + std::to_string(static_cast<std::int64_t>(returnValue)));
#endif
        }
        break;
      }

      case MYSQL_TYPE_TIMESTAMP:    // Needed
      {
        DEBUGMESSAGE("Column Type: TIMESTAMP");

        break;
      }
      case MYSQL_TYPE_DATE:         // Needed
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: DATE");
#endif

        returnValue = Wt::WDate::fromString(val, "yyyy-MM-dd");
        break;
      }
      case MYSQL_TYPE_TIME:         // Needed
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: TIME");
#endif

        returnValue = Wt::WTime::fromString(val, "hh:mm:ss");
        break;
      }
      case MYSQL_TYPE_DATETIME:     // Needed
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: DATETIME");
#endif

        returnValue = Wt::WDateTime::fromString(val, "yyyy-MM-dd hh:mm:ss");
        break;
      }
      case MYSQL_TYPE_BIT:          // Needed
      {
#ifdef DEBUG_ON
        DEBUGMESSAGE("Column Type: BIT");
#endif

          /* A bit column stores bit values. This can be a single bit, or multipe number of bits.
           * The columnLength stores the number of bits present. (1-64) The values are stored as a pure number.
           * IE, the number is not converted to text.
           *
           * The result is converted to a boost::dynamic_bitset for storage in the variant.
           *
           * The value is calculated by left-shifting and adding.
           */

        std::uint_fast8_t nob = static_cast<std::uint_fast8_t>(columnLength);
        std::uint8_t c;
        std::uint64_t value = 0;

        while (nob > 0)
        {
          c = *columnValue;
          value <<= 8;
          value += c;

          nob = (nob > 8 ? nob - 8 : 0);
        };

        returnValue = boost::dynamic_bitset<>(static_cast<std::uint_fast8_t>(columnLength), value);

#ifdef DEBUG_ON
        DEBUGMESSAGE("Bit Value: " + std::to_string(value));
#endif
        break;
      }
      case MYSQL_TYPE_VARCHAR:
      {
        //DEBUGMESSAGE("Column Type: VARCHAR");

        returnValue = std::move(val);
        break;
      }
      case MYSQL_TYPE_TINY_BLOB:    // Needed
      {
        DEBUGMESSAGE("Column Type: TINYBLOB");
        break;
      }
      case MYSQL_TYPE_MEDIUM_BLOB:  // Needed
      {
        DEBUGMESSAGE("Column Type: MEDIUMBLOB");
        break;
      }
      case MYSQL_TYPE_LONG_BLOB:    // Needed
      {
        DEBUGMESSAGE("Column Type: LONGBLOB");
        break;
      }
      case MYSQL_TYPE_BLOB:         // Needed
      {
        DEBUGMESSAGE("Column Type: BLOB");
        break;
      }
      case MYSQL_TYPE_VAR_STRING:
      {
        //DEBUGMESSAGE("Column Type: VARSTRING");

        returnValue = val;
        break;
      }
      case MYSQL_TYPE_NEWDECIMAL:
      {
        //DEBUGMESSAGE("Column Type: NEWDECIMAL");

        returnValue = decimal_t{val};
        break;
      }
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_JSON:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_NULL:
      default:
      {
        CODE_ERROR();
      }
    }

#ifdef DEBUG_ON
    DEBUGMESSAGE("--- Finish Column ---");
#endif

    return returnValue;
  }
}
