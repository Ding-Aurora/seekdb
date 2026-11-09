// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "ai_column_retry.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/engine/expr/ai_column_sql_utils.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include <cctype>

namespace oceanbase { namespace sql {
using namespace common;

static std::string retry_predicate(ObString source, const std::string &state_column, bool no_backslash)
{
  const auto input = AiColumnSqlUtils::text(source);
  std::string result;
  char quote = 0;
  bool line = false;
  bool block = false;
  for (size_t i = 0; i < input.size();) {
    const char c = input[i];
    const char next = i + 1 < input.size() ? input[i + 1] : 0;
    if (quote != 0) {
      result += c; ++i;
      if (c == '\\' && quote != '`' && !no_backslash && i < input.size()) { result += input[i++]; }
      else if (c == quote) { if (i < input.size() && input[i] == quote) { result += input[i++]; } else { quote = 0; } }
    } else if (line) {
      result += c; ++i;
      if (c == '\n' || c == '\r') { line = false; }
    } else if (block) {
      result += c; ++i;
      if (c == '*' && next == '/') { result += input[i++]; block = false; }
    } else if (c == '\'' || c == '"' || c == '`') { quote = c; result += c; ++i; }
    else if (c == '#' || (c == '-' && next == '-' && i + 2 < input.size() && isspace(static_cast<unsigned char>(input[i + 2])))) { line = true; result += c; ++i; }
    else if (c == '/' && next == '*') { block = true; result += c; ++i; }
    else if (isalpha(static_cast<unsigned char>(c)) || c == '_' || static_cast<unsigned char>(c) >= 128) {
      const auto begin = i++;
      while (i < input.size() && (isalnum(static_cast<unsigned char>(input[i])) || input[i] == '_' || input[i] == '$' || static_cast<unsigned char>(input[i]) >= 128)) { ++i; }
      const auto word = input.substr(begin, i - begin);
      size_t previous = begin;
      while (previous > 0 && isspace(static_cast<unsigned char>(input[previous - 1]))) { --previous; }
      size_t following = i;
      while (following < input.size() && isspace(static_cast<unsigned char>(input[following]))) { ++following; }
      if (ObString(word.size(), word.data()).case_compare("status") == 0
          && (previous == 0 || input[previous - 1] != '.') && (following == input.size() || input[following] != '.')) {
        result += "(CASE " + state_column + " WHEN 1 THEN 'READY' WHEN 2 THEN 'FAILED' ELSE 'PENDING' END)";
      } else { result += word; }
    } else { result += c; ++i; }
  }
  return result.empty() ? "1" : result;
}

int AiColumnRetry::execute(ObExecContext &context, ObAlterTableStmt &statement)
{
  int ret = OB_SUCCESS;
  const char *phase = "resolve table";
  DEFER(if (ret != OB_SUCCESS) { FORWARD_USER_ERROR_MSG(ret, "AI COLUMN RETRY failed at %s (%d)", phase, ret); });
  auto *session = context.get_my_session();
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table = nullptr;
  const auto id = statement.ai_column_retry_id_;
  if (session == nullptr || GCTX.schema_service_ == nullptr || id == 0) { return OB_INVALID_ARGUMENT; }
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) { return ret; }
  if (OB_FAIL(schema_guard.get_table_schema(statement.get_alter_table_schema().get_table_id(), table))) { return ret; }
  const auto *column = table == nullptr ? nullptr : table->get_column_schema(id);
  if (column == nullptr || !column->is_ai_column()) { return OB_NOT_SUPPORTED; }
  ObArenaAllocator allocator(ObMemAttr("AiColRetry"));
  ObJsonObject *definition = nullptr;
  if (OB_FAIL(AiColumnSqlUtils::parse(allocator, column->get_ai_column_definition(), definition))) { return ret; }
  const auto state = AiColumnSqlUtils::hidden(*table, *definition, "state_column_id");
  if (state.empty()) { return OB_INVALID_DATA; }
  const auto relation = AiColumnSqlUtils::identifier(AiColumnSqlUtils::text(statement.get_org_database_name())) + "."
      + AiColumnSqlUtils::identifier(AiColumnSqlUtils::text(statement.get_org_table_name()));
  bool no_backslash = false;
  IS_NO_BACKSLASH_ESCAPES(session->get_sql_mode(), no_backslash);
  const auto sql = "UPDATE /*+ USE_PLAN_CACHE(NONE) opt_param('HIDDEN_COLUMN_VISIBLE','true') */ " + relation + " SET "
      + AiColumnSqlUtils::identifier(AiColumnSqlUtils::text(column->get_column_name_str())) + "=NULL WHERE "
      + retry_predicate(statement.ai_column_retry_predicate_, state, no_backslash);
  sqlclient::ObISQLConnectionGuard guard;
  phase = "open connection";
  if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, guard))) { return ret; }
  auto *connection = as_inner_sql_connection(guard.get_ptr());
  if (connection == nullptr) { return OB_ERR_UNEXPECTED; }
  connection->set_check_priv(true);
  auto *old_connection = session->get_inner_conn();
  session->set_inner_conn(guard.get_ptr());
  ObBasicSessionInfo::UserScopeGuard scope(session->get_sql_scope_flags());
  phase = "start transaction";
  if (OB_SUCC(ret = connection->start_transaction())) {
    const auto old_retry = session->get_ai_column_retry_target();
    session->set_ai_column_retry_target({table->get_table_id(), id, session->get_nested_count()});
    int64_t rows = 0;
    phase = "schedule selected rows";
    ret = connection->execute_write(sql.c_str(), rows, true);
    session->set_ai_column_retry_target(old_retry);
    const int end_ret = OB_SUCC(ret) ? connection->commit() : connection->rollback();
    if (OB_SUCC(ret)) { phase = "commit transaction"; ret = end_ret; }
  }
  session->set_inner_conn(old_connection);
  return ret;
}
} }
