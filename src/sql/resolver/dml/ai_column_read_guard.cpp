// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_RESV
#include "ob_dml_resolver.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "sql/engine/expr/ai_column_sql_utils.h"

namespace oceanbase { namespace sql {
using namespace common;
using namespace share::schema;

int ObDMLResolver::check_ai_column_reference(ObRawExpr *expr, bool assignment)
{
  int ret = OB_SUCCESS;
  if (expr == nullptr || !expr->is_column_ref_expr() || session_info_ == nullptr
      || !session_info_->is_user_session() || session_info_->is_ai_column_internal_write()) { return ret; }
  const auto &reference = *static_cast<ObColumnRefRawExpr *>(expr);
  uint64_t column_id = reference.get_column_id();
  const auto *item = get_stmt()->get_table_item_by_id(reference.get_table_id());
  for (auto *parent = get_parent_namespace_resolver(); item == nullptr && parent != nullptr;
       parent = parent->get_parent_namespace_resolver()) {
    if (parent->get_stmt() != nullptr) { item = parent->get_stmt()->get_table_item_by_id(reference.get_table_id()); }
  }
  // An updatable view can rename a directly projected result column.
  // Follow that projection before deciding whether it is writable.
  while (assignment && item != nullptr && !item->is_basic_table() && item->ref_query_ != nullptr
         && column_id >= OB_APP_MIN_COLUMN_ID) {
    const auto index = column_id - OB_APP_MIN_COLUMN_ID;
    auto *select = item->ref_query_;
    if (index >= static_cast<uint64_t>(select->get_select_item_size())) { break; }
    auto *projection = select->get_select_item(index).expr_;
    if (projection == nullptr || !projection->is_column_ref_expr()) { break; }
    auto *base = static_cast<ObColumnRefRawExpr *>(projection);
    item = select->get_table_item_by_id(base->get_table_id());
    column_id = base->get_column_id();
  }
  const ObTableSchema *table = nullptr;
  if (item == nullptr || !item->is_basic_table()) { return ret; }
  if (OB_FAIL(schema_checker_->get_table_schema(item->ref_id_, table))) { return ret; }
  const auto *column = table == nullptr ? nullptr : table->get_column_schema(column_id);
  if (assignment && column != nullptr && column->is_hidden()) {
    const auto name = AiColumnSqlUtils::text(column->get_column_name_str());
    if (name.rfind("__ai_state_", 0) == 0 || name.rfind("__ai_request_", 0) == 0
        || name.rfind("__ai_result_spec_", 0) == 0) { return OB_NOT_SUPPORTED; }
  }
  if (column == nullptr || !column->is_ai_column()) { return ret; }
  if (session_info_->get_ai_column_retry_id(table->get_table_id()) == column->get_column_id()) { return ret; }
  const_cast<ObGlobalHint &>(get_stmt()->get_query_ctx()->get_global_hint()).merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  if (assignment) { return OB_NOT_SUPPORTED; }
  bool strict = false;
  if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_AI_COLUMN_STRICT_READ, strict)) || !strict) { return ret; }
  const ObDatabaseSchema *database = nullptr;
  if (OB_FAIL(schema_checker_->get_schema_guard()->get_database_schema(table->get_database_id(), database))) { return ret; }
  if (database == nullptr) { return OB_ERR_BAD_DATABASE; }
  ObSessionPrivInfo privilege;
  bool visible = false;
  if (OB_FAIL(session_info_->get_session_priv_info(privilege))) { return ret; }
  if (OB_FAIL(schema_checker_->check_table_show(privilege, session_info_->get_enable_role_array(),
            database->get_database_name_str(), table->get_table_name_str(), visible)) || !visible) { return ret; }
  const auto relation = AiColumnSqlUtils::identifier(AiColumnSqlUtils::text(database->get_database_name_str()))
      + "." + AiColumnSqlUtils::identifier(AiColumnSqlUtils::text(table->get_table_name_str()));
  ObArenaAllocator allocator(ObMemAttr("AiColStrict"));
  ObJsonObject *definition = nullptr;
  if (OB_FAIL(AiColumnSqlUtils::parse(allocator, column->get_ai_column_definition(), definition))) { return ret; }
  const auto state = AiColumnSqlUtils::hidden(*table, *definition, "state_column_id");
  const auto spec = AiColumnSqlUtils::hidden(*table, *definition, "result_spec_column_id");
  if (state.empty() || spec.empty()) { return OB_INVALID_DATA; }
  const auto query = "SELECT /*+ USE_PLAN_CACHE(NONE) opt_param('HIDDEN_COLUMN_VISIBLE','true') */ 1 FROM " + relation
      + " WHERE " + state + "<>1 OR " + spec + " IS NULL OR "
      + spec + "<>" + AiColumnSqlUtils::literal(AiColumnSqlUtils::result_spec(*definition)) + " LIMIT 1";
  const bool autocommit = session_info_->get_local_autocommit();
  const auto trans_type = session_info_->get_trans_type();
  const auto query_start = session_info_->get_query_start_time();
  sqlclient::ObISQLConnectionGuard connection_guard;
  if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session_info_, connection_guard))) { return ret; }
  auto *connection = connection_guard.get_ptr();
  if (connection == nullptr) { return OB_ERR_UNEXPECTED; }
  bool pending = false;
  session_info_->enter_ai_column_internal_write();
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(connection->execute_read(query.c_str(), result))) {
    } else if (OB_FAIL(result.get_result()->next())) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    } else { pending = true; }
  }
  connection_guard.reset();
  session_info_->leave_ai_column_internal_write();
  // A resolver probe borrows the caller's transaction. Keep its execution
  // settings intact when returning to the outer statement, even on error.
  const int restore_ret = session_info_->set_autocommit(autocommit);
  session_info_->set_trans_type(trans_type);
  session_info_->set_query_start_time(query_start);
  if (OB_SUCC(ret)) { ret = restore_ret; }
  if (OB_SUCC(ret) && pending) {
    ret = OB_AI_COLUMN_NOT_READY;
    LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, table->get_table_id(), column->get_column_id());
  }
  return ret;
}
} }
