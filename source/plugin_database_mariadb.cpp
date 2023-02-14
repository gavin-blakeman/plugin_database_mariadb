#include "include/pluginInterface/interfaceDatabase.h"

#include "mysql/mysql.h"

#include "include/database_mariadb.h"


namespace pluginInterface
{

  void __attribute__ ((constructor)) initialisePlugin()
  {
    mysql_library_init(0, nullptr, nullptr); ///@todo This may need to move to the constructor or factory function.
  }

  void __attribute__ ((destructor)) destroyPlugin()
  {
    mysql_library_end();
  }

  /// @brief Returns a raw pointer. The raw pointer is owned by the calling function.
  /// @param[in] poolSize: The size of the pool to create.
  /// @param[in] cr: Configuration reader for the application.
  /// @returns A raw (unowned) pointer.
  /// @throws
  /// @version 2022-09-27/GGB - Function created.

  database::CConnectionPool *createConnectionPool(database::handle_t poolSize, GCL::CReaderSections *cr)
  {
    return database::CMariaDBConnector::createDatabaseConnector(poolSize, cr);
  }


  /// @brief Deletes an instance of a connection pool. This function should also be provided as the custom deleter for the
  ///         unique_ptr in the calling function.
  /// @param[in] toDelete: The instance to delete.
  /// @version 2022-09-27/GGB - Function created.

  void destroyConnectionPool(database::CConnectionPool *toDelete)
  {
    delete toDelete;
  }

} // namespace
