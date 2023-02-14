#ifndef DATABASE_MARIADB_H
#define DATABASE_MARIADB_H

  // Standard C++ libraries

#include <memory>
#include <vector>

  // Miscellaneous libraries

#include "mysql/mysql.h"

  // engineeringShop

#include "include/database/database//pluginDatabase.h"

namespace database
{
  class CMariaDBConnector : public CConnectionPool
  {
  private:
    struct connection_t
    {
      MYSQL *mysql;
      MYSQL_RES *mysql_res;
      MYSQL_FIELD *mysql_field;
      MYSQL_ROW mysql_row;
      unsigned int columnCount;
      unsigned long *columnLengths;
      std::uint64_t rowCount;
      std::uint64_t rowCursorActual;    // Cursor posision in recordset
      std::uint64_t rowCursorRequested; // Cursor position in query
      union
      {
        struct
        {
          int connectedFlag     : 1;
          int validRecord       : 1;
          int prepareStatement  : 1;
          int tip               : 1; ///< Transaction in process.
        };
        std::uint64_t v;
      };
      MYSQL_STMT *mysql_stmt;
      std::string preparedStatement;
      std::unique_ptr<MYSQL_BIND[]> mysql_bind;
      std::vector<CVariant> inputParameters;
      std::vector<CVariant> outputParameters;

    };

    std::vector<connection_t> connectionPool;

    virtual void processConnect() override {}   // not implemented. Connections are created as needed.

    virtual void processBeginTransaction(handle_t) override;
    virtual void processEndTransaction(handle_t) override;
    virtual void processCommitTransaction(handle_t) override;
    virtual void processRollbackTransaction(handle_t) override;
    virtual void processQuery(handle_t, std::string const &) override;
    virtual bool processMoveFirst(handle_t) override;
    virtual bool processMoveNext(handle_t) override;
    virtual bool processMovePrevious(handle_t) override;
    virtual bool processMoveLast(handle_t) override {};
    virtual void processGetRecord(handle_t, ::database::CRecord &) override;
    virtual void processGetRecordSet(handle_t, ::database::CRecordSet &) override;
    virtual bool processPrepareQuery(handle_t, std::string const &) override;
    virtual void processAddBindValue(handle_t, CVariant const &) override;
    virtual void processExec(handle_t) override;

    void processResults(handle_t);
    void loadRow(handle_t);
    std::string processError(handle_t);
    ::database::CVariant processColumnValue(handle_t, std::size_t);
    void createInputParameters(handle_t);

  protected:
  public:
    CMariaDBConnector(handle_t);
    virtual ~CMariaDBConnector();

    static CConnectionPool *createDatabaseConnector(handle_t, GCL::CReaderSections *cr);

    friend class ::database::CRecord;

  };

} // namespace

#endif // DATABASE_MARIADB_H
