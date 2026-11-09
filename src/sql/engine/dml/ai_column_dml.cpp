// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "ai_column_dml.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/expr/ai_column_sql_utils.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "share/schema/ob_schema_getter_guard.h"
#include <vector>

namespace oceanbase { namespace sql {
using namespace common;
using namespace share::schema;
using AiSql = AiColumnSqlUtils;

struct AiCaptureError {
  int &status;
  const char *stage = "schema";
  ~AiCaptureError()
  {
    const int ret = status;
    if (ret != OB_SUCCESS) { FORWARD_USER_ERROR_MSG(ret, "AI COLUMN capture failed at %s (%d)", stage, ret); }
  }
};

bool AiColumnDml::needs_after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef)
{
  auto *session = op.get_exec_ctx().get_my_session();
  return ctdef.has_ai_column_ && ctdef.is_primary_index_ && session != nullptr
      && session->is_user_session() && !session->is_ai_column_internal_write();
}

static int ai_rowkey(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef,
    const ObTableSchema &table, const ObExprPtrIArray &row, std::string &predicate)
{
  int ret = OB_SUCCESS;
  char *buffer = nullptr;
  int64_t capacity = 0;
  int64_t position = 0;
  ObSEArray<uint64_t, 4> ids;
  if (OB_FAIL(table.get_rowkey_column_ids(ids))) { return ret; }
  for (int64_t k = 0; OB_SUCC(ret) && k < ids.count(); ++k) {
    int64_t index = -1;
    for (int64_t i = 0; i < ctdef.column_ids_.count(); ++i) { if (ctdef.column_ids_.at(i) == ids.at(k)) { index = i; break; } }
    const auto *column = table.get_column_schema(ids.at(k));
    if (index < 0 || index >= row.count() || column == nullptr) { ret = OB_ERR_UNEXPECTED; }
    else {
      ObDatum *datum = nullptr;
      ObObj value;
      const auto name = AiSql::identifier(AiSql::text(column->get_column_name_str()));
      if (OB_FAIL(row.at(index)->eval(op.get_eval_ctx(), datum))) {
      } else if (OB_FAIL(datum->to_obj(value, row.at(index)->obj_meta_))) {
      } else if (OB_FAIL(databuff_printf(buffer, capacity, position, op.get_exec_ctx().get_allocator(), "%s%s = ", k == 0 ? "" : " and ", name.c_str()))) {
      } else if (value.is_string_type()) {
        auto literal = AiSql::literal(AiSql::text(value.get_string()));
        if (column->get_collation_type() != CS_TYPE_BINARY) {
          literal = "CONVERT(" + literal + " USING " + ObCharset::charset_name(column->get_charset_type()) + ")";
        }
        ret = databuff_printf(buffer, capacity, position, op.get_exec_ctx().get_allocator(), "%s", literal.c_str());
      } else if (OB_FAIL(value.print_sql_literal(buffer, capacity, position,
                     op.get_exec_ctx().get_allocator(), op.get_obj_print_params()))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (position <= 0 || position > 4096) { ret = OB_SIZE_OVERFLOW; }
    else { predicate.assign(buffer, position); }
  }
  return ret;
}

static int ai_dependency_changed(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef,
                                 uint64_t id, bool &changed)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !changed && i < ctdef.column_ids_.count(); ++i) {
    if (ctdef.column_ids_.at(i) != id) { continue; }
    if (i >= ctdef.old_row_.count() || i >= ctdef.new_row_.count()) { ret = OB_ERR_UNEXPECTED; }
    else {
      ObDatum *before = nullptr;
      ObDatum *after = nullptr;
      ObObj old_value;
      ObObj new_value;
      if (OB_FAIL(ctdef.old_row_.at(i)->eval(op.get_eval_ctx(), before))) {
      } else if (OB_FAIL(ctdef.new_row_.at(i)->eval(op.get_eval_ctx(), after))) {
      } else if (OB_FAIL(before->to_obj(old_value, ctdef.old_row_.at(i)->obj_meta_))) {
      } else if (OB_FAIL(after->to_obj(new_value, ctdef.new_row_.at(i)->obj_meta_))) {
      } else if (old_value.is_string_type() && new_value.is_string_type()) {
        changed = old_value.get_string() != new_value.get_string();
      } else { changed = old_value != new_value; }
    }
  }
  return ret;
}

int AiColumnDml::after_row(ObDMLModifyRowNode &row)
{
  int ret = OB_SUCCESS;
  if (row.dml_op_ == nullptr || row.dml_ctdef_ == nullptr) { return OB_ERR_UNEXPECTED; }
  auto &op = *row.dml_op_;
  const auto &ctdef = *row.dml_ctdef_;
  if (!needs_after_row(op, ctdef)) { return ret; }
  AiCaptureError error{ret};
  auto *session = op.get_exec_ctx().get_my_session();
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  auto *guard = op.get_exec_ctx().get_sql_ctx()->schema_guard_;
  if (guard == nullptr || session == nullptr) { return OB_ERR_UNEXPECTED; }
  if (OB_FAIL(guard->get_table_schema(ctdef.das_base_ctdef_.index_tid_, table))) { return ret; }
  if (table == nullptr) { return OB_TABLE_NOT_EXIST; }
  const bool inserting = row.dml_event_ == ObDmlEventType::DE_INSERTING;
  const bool deleting = row.dml_event_ == ObDmlEventType::DE_DELETING;
  std::string old_key;
  std::string new_key;
  error.stage = "row identity";
  if (!inserting && OB_FAIL(ai_rowkey(op, ctdef, *table, ctdef.old_row_, old_key))) { return ret; }
  if (!deleting && OB_FAIL(ai_rowkey(op, ctdef, *table, ctdef.new_row_, new_key))) { return ret; }
  std::vector<const ObColumnSchemaV2 *> affected;
  error.stage = "dependencies";
  ObArenaAllocator allocator(ObMemAttr("AiColumnDml"));
  for (auto it = table->column_begin(); OB_SUCC(ret) && it != table->column_end(); ++it) {
    if (!(*it)->is_ai_column() || (*it)->is_unused()) { continue; }
    const auto retry_id = session->get_ai_column_retry_id(table->get_table_id());
    if (retry_id != 0 && retry_id != (*it)->get_column_id()) { continue; }
    bool changed = retry_id != 0 || inserting || deleting || old_key != new_key;
    ObJsonObject *definition = nullptr;
    if (OB_FAIL(AiSql::parse(allocator, (*it)->get_ai_column_definition(), definition))) { break; }
    auto *dependencies = definition->get_value("dependency_ids");
    if (dependencies == nullptr) { ret = OB_INVALID_DATA; break; }
    for (int64_t i = 0; OB_SUCC(ret) && !changed && i < dependencies->element_count(); ++i) {
      ret = ai_dependency_changed(op, ctdef, dependencies->get_value(i)->get_int(), changed);
    }
    if (changed) { affected.push_back(*it); }
  }
  if (OB_FAIL(ret) || affected.empty()) { return ret; }
  if (OB_FAIL(guard->get_database_schema(table->get_database_id(), database))) { return ret; }
  if (database == nullptr) { return OB_ERR_BAD_DATABASE; }
  const auto relation = AiSql::identifier(AiSql::text(database->get_database_name_str())) + "." + AiSql::identifier(AiSql::text(table->get_table_name_str()));
  const auto table_id = std::to_string(table->get_table_id());
  char uuid[UuidCommon::LENGTH_UUID + 1] = {};
  if (!deleting && OB_FAIL(ObExprUuid::gen_server_uuid(uuid, UuidCommon::LENGTH_UUID))) { return ret; }
  const auto request = AiSql::literal(uuid);
  error.stage = "nested connection";
  if (OB_FAIL(op.check_stack())) { return ret; }
  if (OB_FAIL(op.open_inner_conn())) { return ret; }
  if (OB_FAIL(op.begin_nested_session(true))) { return ret; }
  session->enter_ai_column_internal_write();
  std::string assignments;
  error.stage = "discard old tasks";
  for (const auto *column : affected) {
    if (OB_FAIL(ret)) { break; }
    const auto id = std::to_string(column->get_column_id());
    const auto key = deleting ? old_key : new_key;
    std::string sql = "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=" + table_id
        + " AND column_id=" + id + " AND (rowkey_predicate=" + AiSql::literal(key);
    if (!old_key.empty() && old_key != key) { sql += " OR rowkey_predicate=" + AiSql::literal(old_key); }
    sql += ")";
    error.stage = "delete task SQL";
    if (OB_FAIL(op.execute_write(sql.c_str()))) { break; }
    if (!deleting) {
      error.stage = "result specification";
      ObJsonObject *definition = nullptr;
      if (OB_FAIL(AiSql::parse(allocator, column->get_ai_column_definition(), definition))) { break; }
      const auto state = AiSql::hidden(*table, *definition, "state_column_id");
      const auto request_column = AiSql::hidden(*table, *definition, "request_column_id");
      const auto spec = AiSql::hidden(*table, *definition, "result_spec_column_id");
      if (state.empty() || request_column.empty() || spec.empty()) { ret = OB_INVALID_DATA; break; }
      if (!assignments.empty()) { assignments += ","; }
      assignments += AiSql::identifier(AiSql::text(column->get_column_name_str())) + "=NULL,"
          + state + "=0," + request_column + "=" + request + "," + spec + "=" + AiSql::literal(AiSql::result_spec(*definition));
    }
  }
  if (OB_SUCC(ret) && !deleting) {
    error.stage = "invalidate result";
    const auto sql = "UPDATE /*+ USE_PLAN_CACHE(NONE) opt_param('HIDDEN_COLUMN_VISIBLE','true') */ " + relation + " SET " + assignments + " WHERE " + new_key;
    ret = op.execute_write(sql.c_str());
  }
  for (const auto *column : affected) {
    if (OB_FAIL(ret) || deleting) { break; }
    ObJsonObject *definition = nullptr;
    if (OB_FAIL(AiSql::parse(allocator, column->get_ai_column_definition(), definition))) { break; }
    const auto id = std::to_string(column->get_column_id());
    const auto input = AiSql::field(*definition, "input_expr");
    const auto config = AiSql::field(*definition, "config_expr");
    auto *retries = definition->get_value("max_retries");
    auto *version = definition->get_value("definition_version");
    if (input.empty() || config.empty() || retries == nullptr || version == nullptr) { ret = OB_INVALID_DATA; break; }
    std::string input_value;
    std::string config_value;
    bool input_null = false;
    bool config_null = false;
    ObObjMeta input_type;
    const auto saved_sql_mode = session->get_sql_mode();
    error.stage = "capture input";
    auto *definition_mode = definition->get_value("sql_mode");
    if (definition_mode != nullptr) { session->set_sql_mode(definition_mode->get_int()); }
    {
      const auto query = "SELECT /*+ opt_param('HIDDEN_COLUMN_VISIBLE','true') */ CAST((" + input + ") AS CHAR) AS input_value,CAST((" + config
          + ") AS CHAR) AS config_value,(" + input + ") AS input_type FROM " + relation + " WHERE " + new_key;
      ObMySQLProxy::MySQLResult result;
      if (OB_FAIL(op.execute_read(query.c_str(), result))) {
      } else if (OB_FAIL(result.get_result()->next())) {
      } else {
        ObString value;
        ret = result.get_result()->get_varchar("input_value", value);
        if (ret == OB_ERR_NULL_VALUE) { input_null = true; ret = OB_SUCCESS; }
        else if (OB_SUCC(ret)) { input_value = AiSql::text(value); }
        if (OB_SUCC(ret)) {
          ret = result.get_result()->get_varchar("config_value", value);
          if (ret == OB_ERR_NULL_VALUE) { config_null = true; ret = OB_SUCCESS; }
          else if (OB_SUCC(ret)) { config_value = AiSql::text(value); }
        }
        if (OB_SUCC(ret)) { ret = result.get_result()->get_type("input_type", input_type); }
      }
    }
    session->set_sql_mode(saved_sql_mode);
    if (OB_SUCC(ret)) {
      error.stage = "insert task";
      const auto sql = "INSERT INTO oceanbase.__all_ai_column_task(table_id,request_id,column_id,definition_version,rowkey_predicate,input_value,input_is_json,config_value,status,max_retries,eligible_ts) VALUES ("
          + table_id + "," + request + "," + id + "," + std::to_string(version->get_int()) + "," + AiSql::literal(new_key)
          + "," + (input_null ? "NULL" : AiSql::literal(input_value)) + "," + (input_type.is_json() ? "1" : "0")
          + "," + (config_null ? "NULL" : AiSql::literal(config_value)) + ",'PENDING'," + std::to_string(retries->get_int()) + ",now(6))";
      ret = op.execute_write(sql.c_str());
    }
  }
  session->leave_ai_column_internal_write();
  const int end_ret = op.end_nested_session();
  if (OB_SUCC(ret)) { ret = end_ret; }
  return ret;
}
} }
