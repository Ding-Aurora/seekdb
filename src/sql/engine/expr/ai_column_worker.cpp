// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "ai_column_worker.h"
#include "ai_column_sql_utils.h"
#include "ob_expr_uuid.h"
#include "ob_expr_ai/ob_ai_func_utils.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast.h"
#include <charconv>
#include <cmath>
#include <cerrno>
#include <vector>
#include <unistd.h>

namespace oceanbase { namespace sql {
using namespace common;
using namespace share::schema;

namespace {
using SqlText = AiColumnSqlUtils;
struct ColumnJob {
  int64_t table_id = 0;
  int64_t column_id = 0;
  int64_t version = 0;
  int64_t epoch = 0;
  int64_t attempts = 0;
  int64_t max_retries = 0;
  bool input_null = false;
  bool input_is_json = false;
  bool result_null = true;
  std::string request;
  std::string key;
  std::string input;
  std::string config;
  std::string status;
  std::string output;
};

int job_string(sqlclient::ObMySQLResult &result, const char *name, std::string &value, bool *is_null = nullptr)
{
  ObString string;
  int ret = result.get_varchar(name, string);
  if (ret == OB_ERR_NULL_VALUE) { if (is_null != nullptr) { *is_null = true; } value.clear(); ret = OB_SUCCESS; }
  else if (ret == OB_SUCCESS) { if (is_null != nullptr) { *is_null = false; } value = SqlText::text(string); }
  return ret;
}

int read_job(sqlclient::ObMySQLResult &result, ColumnJob &job)
{
  int ret = OB_SUCCESS;
  OZ(result.get_int("table_id", job.table_id));
  OZ(result.get_int("column_id", job.column_id));
  OZ(result.get_int("definition_version", job.version));
  OZ(result.get_int("lease_epoch", job.epoch));
  OZ(result.get_int("attempts", job.attempts));
  OZ(result.get_int("max_retries", job.max_retries));
  OZ(job_string(result, "request_id", job.request));
  OZ(job_string(result, "rowkey_predicate", job.key));
  OZ(job_string(result, "input_value", job.input, &job.input_null));
  int64_t input_json = 0;
  OZ(result.get_int("input_is_json", input_json));
  job.input_is_json = input_json != 0;
  OZ(job_string(result, "config_value", job.config));
  OZ(job_string(result, "status", job.status));
  OZ(job_string(result, "result_value", job.output));
  int64_t null_value = 1;
  OZ(result.get_int("result_is_null", null_value));
  job.result_null = null_value != 0;
  return ret;
}

std::string job_filter(const ColumnJob &job)
{
  return "table_id=" + std::to_string(job.table_id) + " AND request_id=" + SqlText::literal(job.request)
      + " AND column_id=" + std::to_string(job.column_id) + " AND definition_version=" + std::to_string(job.version);
}

int job_write(ObISQLClient &client, const std::string &sql, int64_t *affected = nullptr)
{
  int64_t rows = 0;
  int ret = client.write(sql.c_str(), rows);
  if (affected != nullptr) { *affected = rows; }
  return ret;
}

int job_write_result(ObMySQLTransaction &transaction, const std::string &sql)
{
  auto *session = query::ObInnerSQLConnectionAccess::get_session(transaction.get_connection());
  if (session != nullptr) { session->enter_ai_column_internal_write(); }
  const int ret = job_write(transaction, sql);
  if (session != nullptr) { session->leave_ai_column_internal_write(); }
  return ret;
}

int column_relation(const ObTableSchema &table, ObSchemaGetterGuard &guard, std::string &relation)
{
  const ObDatabaseSchema *database = nullptr;
  int ret = guard.get_database_schema(table.get_database_id(), database);
  if (OB_SUCC(ret)) {
    if (database == nullptr) { ret = OB_TABLE_NOT_EXIST; }
    else { relation = SqlText::identifier(SqlText::text(database->get_database_name_str())) + "." + SqlText::identifier(SqlText::text(table.get_table_name_str())); }
  }
  return ret;
}

bool current_column_job(ObIAllocator &allocator, const ObColumnSchemaV2 *column, const ColumnJob &job)
{
  ObJsonObject *definition = nullptr;
  if (column == nullptr || !column->is_ai_column() || column->is_unused()
      || SqlText::parse(allocator, column->get_ai_column_definition(), definition) != OB_SUCCESS) { return false; }
  const auto *version = definition->get_value("definition_version");
  return version != nullptr && version->get_int() == job.version;
}

// Reject malformed typed output before recording a successful attempt.
int validate_column_output(ObIAllocator &allocator, const ObColumnSchemaV2 &column,
                           ObJsonObject &definition, const std::string &output)
{
  int ret = OB_SUCCESS;
  if (column.is_json() || column.is_collection()) {
    ObIJsonBase *json = nullptr;
    ret = ObJsonBaseFactory::get_json_base(&allocator, ObString(output.size(), output.data()),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json);
    if (OB_SUCC(ret) && column.is_collection()) {
      const auto *dimension = definition.get_value("dimension");
      if (json == nullptr || dimension == nullptr || json->json_type() != ObJsonNodeType::J_ARRAY
          || json->element_count() != dimension->get_int()) { ret = OB_INVALID_DATA; }
      for (int64_t i = 0; OB_SUCC(ret) && i < json->element_count(); ++i) {
        const auto *value = static_cast<ObJsonArray *>(json)->get_value(i);
        const auto kind = value->json_type();
        if (kind != ObJsonNodeType::J_INT && kind != ObJsonNodeType::J_UINT
            && kind != ObJsonNodeType::J_DOUBLE && kind != ObJsonNodeType::J_DECIMAL) { ret = OB_INVALID_DATA; }
      }
    }
  } else if (ob_is_integer_type(column.get_data_type())) {
    auto first = output.data();
    auto last = first + output.size();
    while (first != last && isspace(static_cast<unsigned char>(*first))) { ++first; }
    while (last != first && isspace(static_cast<unsigned char>(last[-1]))) { --last; }
    const bool uns = ob_is_unsigned_type(column.get_data_type());
    uint64_t unsigned_value = 0;
    int64_t signed_value = 0;
    if (uns) {
      auto parsed = std::from_chars(first, last, unsigned_value);
      if (first == last || parsed.ec != std::errc() || parsed.ptr != last) { ret = OB_INVALID_DATA; }
    } else {
      auto parsed = std::from_chars(first, last, signed_value);
      if (first == last || parsed.ec != std::errc() || parsed.ptr != last) { ret = OB_INVALID_DATA; }
    }
    const auto type = column.get_data_type();
    const int bits = type == ObTinyIntType || type == ObUTinyIntType ? 8
        : type == ObSmallIntType || type == ObUSmallIntType ? 16
        : type == ObMediumIntType || type == ObUMediumIntType ? 24
        : type == ObInt32Type || type == ObUInt32Type ? 32 : 64;
    if (OB_SUCC(ret) && bits < 64) {
      if (uns ? unsigned_value >= (uint64_t(1) << bits)
              : signed_value < -(int64_t(1) << (bits - 1)) || signed_value >= (int64_t(1) << (bits - 1))) { ret = OB_DATA_OUT_OF_RANGE; }
    }
  } else if (ob_is_float_type(column.get_data_type()) || ob_is_double_type(column.get_data_type())) {
    char *end = nullptr;
    errno = 0;
    const double value = strtod(output.c_str(), &end);
    while (end != nullptr && isspace(static_cast<unsigned char>(*end))) { ++end; }
    if (end == output.c_str() || end == nullptr || *end != 0 || errno == ERANGE || !std::isfinite(value)) { ret = OB_INVALID_DATA; }
  }
  if (OB_SUCC(ret) && !column.is_json() && !column.is_collection()) {
    ObObj source;
    ObObj converted;
    ObObj checked;
    const ObObj *result = nullptr;
    auto accuracy = column.get_accuracy();
    ObCastCtx context(&allocator, nullptr, CM_STRICT_MODE, column.get_collation_type(), &accuracy);
    source.set_varchar(ObString(output.size(), output.data()));
    source.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObExpectType expected(column.get_data_type(), column.get_collation_type());
    expected.set_type_infos(&column.get_extended_type_info());
    if (OB_FAIL(ObObjCaster::to_type(expected, context, source, converted, result))) {
    } else if (result == nullptr) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObObj value = *result;
      if (OB_FAIL(obj_collation_check(context, true, column.get_collation_type(), value))) {
      } else if (OB_FAIL(obj_accuracy_check(context, accuracy, column.get_collation_type(), value, checked, result))) {
      } else if (context.warning_ != OB_SUCCESS) { ret = context.warning_; }
    }
  }
  return ret;
}

int execute_column_model(const ColumnJob &job, const ObColumnSchemaV2 &column, ObIAllocator &allocator,
                         std::string &output)
{
  int ret = OB_SUCCESS;
  ObJsonObject *definition = nullptr;
  ObJsonObject *config = nullptr;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
  if (OB_FAIL(SqlText::parse(allocator, column.get_ai_column_definition(), definition))) { return ret; }
  std::string name = SqlText::field(*definition, "model");
  ObString model_name(name.size(), name.data());
  if (!job.config.empty() && OB_FAIL(SqlText::parse(allocator, ObString(job.config.size(), job.config.data()), config))) { return ret; }
  if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_name, info))) { return ret; }
  if (resolver == nullptr || info == nullptr) { return OB_NOT_INIT; }
  if (OB_FAIL(resolver->resolve_by_model_name(model_name, allocator, endpoint))) { return ret; }
  ObString input(job.input.size(), job.input.data());
  if (job.input_is_json) {
    ObJsonObject *prompt_object = nullptr;
    if (OB_FAIL(SqlText::parse(allocator, input, prompt_object))) { return ret; }
    if (!ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object)) { return OB_INVALID_ARGUMENT; }
    if (OB_FAIL(ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt_object, input))) { return ret; }
  }
  ObAIFuncModel model(allocator, *info, endpoint);
  ObString result;
  if (SqlText::field(*definition, "function") == "AI_EMBED") {
    if (config == nullptr) { config = OB_NEWx(ObJsonObject, &allocator, &allocator); }
    auto *dimension = definition->get_value("dimension");
    if (config == nullptr || dimension == nullptr) { return OB_INVALID_DATA; }
    auto *number = OB_NEWx(ObJsonInt, &allocator, dimension->get_int());
    if (number == nullptr) { return OB_ALLOCATE_MEMORY_FAILED; }
    if (OB_FAIL(config->add("dimensions", number))) { return ret; }
    ret = model.call_dense_embedding(input, config, result);
  } else {
    const auto prompt = SqlText::field(*definition, "type_prompt");
    ret = model.call_completion(input, config, result, ObString(prompt.size(), prompt.data()));
  }
  if (OB_SUCC(ret)) {
    output = SqlText::text(result);
    ret = validate_column_output(allocator, column, *definition, output);
  }
  return ret;
}

int publish_column_group(ObMySQLProxy &proxy, const ColumnJob &completed)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction transaction;
  ObArenaAllocator allocator(ObMemAttr("AiColumnPublish"));
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  std::string relation;
  if (GCTX.schema_service_ == nullptr) { return OB_NOT_INIT; }
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) { return ret; }
  if (OB_FAIL(guard.get_table_schema(completed.table_id, table))) { return ret; }
  bool current = table != nullptr && !table->is_in_recyclebin();
  if (current && OB_FAIL(column_relation(*table, guard, relation))) { return ret; }
  if (OB_FAIL(transaction.start(&proxy))) { return ret; }
  // Match foreground DML's lock order: business row before task records.
  if (current) {
    ObMySQLProxy::MySQLResult result;
    const auto sql = "SELECT /*+ opt_param('HIDDEN_COLUMN_VISIBLE','true') */ 1 FROM "
        + relation + " WHERE (" + completed.key + ") LIMIT 1 FOR UPDATE";
    if (OB_FAIL(transaction.read(result, sql.c_str()))) {
    } else if (OB_FAIL(result.get_result()->next()) && ret == OB_ITER_END) {
      current = false;
      ret = OB_SUCCESS;
    }
  }
  const auto group = "table_id=" + std::to_string(completed.table_id) + " AND request_id=" + SqlText::literal(completed.request);
  std::vector<ColumnJob> jobs;
  if (OB_SUCC(ret)) {
    ObMySQLProxy::MySQLResult result;
    const auto sql = "SELECT * FROM oceanbase.__all_ai_column_task WHERE " + group + " ORDER BY column_id FOR UPDATE";
    if (OB_FAIL(transaction.read(result, sql.c_str()))) {
    } else {
      int next = OB_SUCCESS;
      while (OB_SUCC(ret) && (next = result.get_result()->next()) == OB_SUCCESS) {
        ColumnJob job;
        if (OB_FAIL(read_job(*result.get_result(), job))) { break; }
        if (job.status != "READY" && job.status != "FAILED") { jobs.clear(); break; }
        jobs.push_back(std::move(job));
      }
      if (OB_SUCC(ret) && next != OB_SUCCESS && next != OB_ITER_END) { ret = next; }
    }
  }
  if (OB_SUCC(ret) && !jobs.empty()) {
    std::string assignments;
    std::string fence;
    for (const auto &job : jobs) {
      if (OB_FAIL(ret) || !current) { break; }
      const auto *column = table->get_column_schema(job.column_id);
      if (job.key != completed.key) { ret = OB_INVALID_DATA; break; }
      // Changing one definition does not discard a sibling column's outcome.
      if (!current_column_job(allocator, column, job)) { continue; }
      ObJsonObject *definition = nullptr;
      if (OB_FAIL(SqlText::parse(allocator, column->get_ai_column_definition(), definition))) { break; }
      const auto state = SqlText::hidden(*table, *definition, "state_column_id");
      const auto request_column = SqlText::hidden(*table, *definition, "request_column_id");
      if (state.empty() || request_column.empty()) { ret = OB_INVALID_DATA; break; }
      if (!assignments.empty()) { assignments += ","; }
      assignments += SqlText::identifier(SqlText::text(column->get_column_name_str())) + "=";
      if (job.status == "FAILED" || job.result_null) { assignments += "NULL"; }
      else {
        const auto value = "CONVERT(" + SqlText::literal(job.output) + " USING utf8mb4)";
        assignments += column->is_json() ? "CAST(" + value + " AS JSON)" : value;
      }
      assignments += "," + state + (job.status == "READY" ? "=1" : "=2");
      fence += " AND " + request_column + "=" + SqlText::literal(job.request);
    }
    if (OB_SUCC(ret) && current && !assignments.empty()) {
      const auto sql = "UPDATE /*+ USE_PLAN_CACHE(NONE) opt_param('HIDDEN_COLUMN_VISIBLE','true') */ " + relation + " SET " + assignments
          + " WHERE (" + completed.key + ")" + fence;
      ret = job_write_result(transaction, sql);
    }
    if (OB_SUCC(ret)) { ret = job_write(transaction, "DELETE FROM oceanbase.__all_ai_column_task WHERE " + group); }
  }
  const int end_ret = transaction.end(OB_SUCC(ret));
  return OB_SUCC(ret) ? end_ret : ret;
}

// Capture pre-existing rows after ADD/MODIFY using the same durable task format
// as foreground DML. A row lock makes capture atomic with concurrent writers.
int backfill_column_row(ObMySQLProxy &proxy, const ObTableSchema &table,
                        const std::string &relation, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObArenaAllocator allocator(ObMemAttr("AiColBackfill"));
  std::vector<const ObColumnSchemaV2 *> columns;
  std::string pending;
  std::string projection;
  for (auto it = table.column_begin(); it != table.column_end(); ++it) {
    if (!(*it)->is_ai_column() || (*it)->is_unused()) { continue; }
    ObJsonObject *definition = nullptr;
    if (OB_FAIL(SqlText::parse(allocator, (*it)->get_ai_column_definition(), definition))) { return ret; }
    const auto id = std::to_string((*it)->get_column_id());
    const auto spec = SqlText::hidden(table, *definition, "result_spec_column_id");
    const auto request_column = SqlText::hidden(table, *definition, "request_column_id");
    if (spec.empty() || request_column.empty()) { return OB_INVALID_DATA; }
    if (!pending.empty()) { pending += " OR "; }
    pending += "(" + spec + " IS NULL OR " + spec + "<>" + SqlText::literal(SqlText::result_spec(*definition)) + ")";
    projection += "," + spec + " AS spec_" + id;
    projection += "," + request_column + " AS request_" + id;
    columns.push_back(*it);
  }
  if (columns.empty()) { return ret; }
  ObSEArray<uint64_t, 4> keys;
  if (OB_FAIL(table.get_rowkey_column_ids(keys))) { return ret; }
  std::string key_expression = "CONCAT(";
  for (int64_t i = 0; i < keys.count(); ++i) {
    const auto *key = table.get_column_schema(keys.at(i));
    if (key == nullptr) { return OB_ERR_UNEXPECTED; }
    const auto identifier = SqlText::identifier(SqlText::text(key->get_column_name_str()));
    if (i != 0) { key_expression += ","; }
    const auto prefix = (i == 0 ? "" : " and ") + identifier + " = ";
    if (ob_is_numeric_type(key->get_data_type())) {
      key_expression += SqlText::literal(prefix) + ",CAST(" + identifier + " AS CHAR)";
    } else if (key->is_string_type() && key->get_collation_type() == CS_TYPE_BINARY) {
      key_expression += SqlText::literal(prefix + "X'") + ",LOWER(HEX(" + identifier + "))," + SqlText::literal("'");
    } else if (key->is_string_type()) {
      key_expression += SqlText::literal(prefix + "CONVERT(X'") + ",LOWER(HEX(" + identifier + ")),"
          + SqlText::literal("' USING " + std::string(ObCharset::charset_name(key->get_charset_type())) + ")");
    } else {
      key_expression += SqlText::literal(prefix + "CONVERT(X'")
          + ",LOWER(HEX(CAST(" + identifier + " AS CHAR)))," + SqlText::literal("' USING utf8mb4)");
    }
  }
  if (keys.empty()) { return OB_NOT_SUPPORTED; }
  key_expression += ")";
  ObMySQLTransaction transaction;
  if (OB_FAIL(transaction.start(&proxy))) { return ret; }
  std::string key;
  std::vector<const ObColumnSchemaV2 *> affected;
  std::vector<std::string> previous_requests;
  {
    ObMySQLProxy::MySQLResult result;
    const auto query = "SELECT /*+ opt_param('HIDDEN_COLUMN_VISIBLE','true') */ " + key_expression + " AS rowkey_predicate"
        + projection + " FROM " + relation + " WHERE " + pending + " LIMIT 1 FOR UPDATE";
    if (OB_FAIL(transaction.read(result, query.c_str()))) {
    } else if (OB_FAIL(result.get_result()->next())) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    } else if (OB_FAIL(job_string(*result.get_result(), "rowkey_predicate", key))) {
    } else {
      found = true;
      for (const auto *column : columns) {
        ObJsonObject *definition = nullptr;
        if (OB_FAIL(SqlText::parse(allocator, column->get_ai_column_definition(), definition))) { break; }
        const auto id = std::to_string(column->get_column_id());
        std::string spec;
        std::string request;
        if (OB_FAIL(job_string(*result.get_result(), ("spec_" + id).c_str(), spec))) { break; }
        if (spec != SqlText::result_spec(*definition)) {
          if (OB_FAIL(job_string(*result.get_result(), ("request_" + id).c_str(), request))) { break; }
          affected.push_back(column);
          previous_requests.push_back(std::move(request));
        }
      }
    }
  }
  char uuid[UuidCommon::LENGTH_UUID + 1] = {};
  if (OB_SUCC(ret) && found) { ret = ObExprUuid::gen_server_uuid(uuid, UuidCommon::LENGTH_UUID); }
  const auto table_id = std::to_string(table.get_table_id());
  const auto request = SqlText::literal(uuid);
  std::string assignments;
  for (size_t i = 0; OB_SUCC(ret) && found && i < affected.size(); ++i) {
    const auto *column = affected[i];
    const auto id = std::to_string(column->get_column_id());
    ObJsonObject *definition = nullptr;
    ColumnJob job;
    if (OB_FAIL(SqlText::parse(allocator, column->get_ai_column_definition(), definition))) { break; }
    const auto *version = definition->get_value("definition_version");
    const auto *retries = definition->get_value("max_retries");
    const auto input = SqlText::field(*definition, "input_expr");
    const auto config = SqlText::field(*definition, "config_expr");
    if (version == nullptr || retries == nullptr || input.empty() || config.empty()) { ret = OB_INVALID_DATA; break; }
    auto *session = query::ObInnerSQLConnectionAccess::get_session(transaction.get_connection());
    const auto old_mode = session == nullptr ? 0 : session->get_sql_mode();
    const auto *mode = definition->get_value("sql_mode");
    if (session != nullptr && mode != nullptr) { session->set_sql_mode(mode->get_int()); }
    {
      ObMySQLProxy::MySQLResult result;
      const auto query = "SELECT /*+ opt_param('HIDDEN_COLUMN_VISIBLE','true') */ CAST((" + input + ") AS CHAR) AS input_value,CAST((" + config
          + ") AS CHAR) AS config_value,(" + input + ") AS input_type FROM " + relation + " WHERE " + key;
      ObObjMeta type;
      if (OB_FAIL(transaction.read(result, query.c_str()))) {
      } else if (OB_FAIL(result.get_result()->next())) {
      } else if (OB_FAIL(job_string(*result.get_result(), "input_value", job.input, &job.input_null))) {
      } else if (OB_FAIL(job_string(*result.get_result(), "config_value", job.config))) {
      } else if (OB_FAIL(result.get_result()->get_type("input_type", type))) {
      } else { job.input_is_json = type.is_json(); }
    }
    if (session != nullptr) { session->set_sql_mode(old_mode); }
    if (OB_SUCC(ret) && !previous_requests[i].empty()) {
      ret = job_write(transaction, "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=" + table_id
          + " AND column_id=" + id + " AND request_id=" + SqlText::literal(previous_requests[i]));
    }
    if (OB_SUCC(ret)) {
      const auto sql = "INSERT INTO oceanbase.__all_ai_column_task(table_id,request_id,column_id,definition_version,rowkey_predicate,input_value,input_is_json,config_value,status,max_retries,eligible_ts) VALUES("
          + table_id + "," + request + "," + id + "," + std::to_string(version->get_int()) + "," + SqlText::literal(key)
          + "," + (job.input_null ? "NULL" : SqlText::literal(job.input)) + "," + (job.input_is_json ? "1" : "0")
          + "," + (job.config.empty() ? "NULL" : SqlText::literal(job.config)) + ",'PENDING'," + std::to_string(retries->get_int()) + ",now(6))";
      ret = job_write(transaction, sql);
    }
    const auto state = SqlText::hidden(table, *definition, "state_column_id");
    const auto request_column = SqlText::hidden(table, *definition, "request_column_id");
    const auto spec = SqlText::hidden(table, *definition, "result_spec_column_id");
    if (state.empty() || request_column.empty() || spec.empty()) { ret = OB_INVALID_DATA; break; }
    if (!assignments.empty()) { assignments += ","; }
    assignments += SqlText::identifier(SqlText::text(column->get_column_name_str())) + "=NULL,"
        + state + "=0," + request_column + "=" + request + "," + spec + "=" + SqlText::literal(SqlText::result_spec(*definition));
  }
  if (OB_SUCC(ret) && found && !assignments.empty()) {
    ret = job_write_result(transaction, "UPDATE /*+ USE_PLAN_CACHE(NONE) opt_param('HIDDEN_COLUMN_VISIBLE','true') */ "
        + relation + " SET " + assignments + " WHERE " + key);
  }
  const int end_ret = transaction.end(OB_SUCC(ret));
  return OB_SUCC(ret) ? end_ret : ret;
}

int backfill_column_tables(ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  ObSEArray<const ObTableSchema *, 16> tables;
  if (GCTX.schema_service_ == nullptr) { return OB_NOT_INIT; }
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) { return ret; }
  if (OB_FAIL(guard.get_table_schemas_in_runtime(tables))) { return ret; }
  for (const auto *table : tables) {
    if (table == nullptr || !table->is_user_table() || table->is_in_recyclebin()) { continue; }
    bool has_ai = false;
    for (auto it = table->column_begin(); it != table->column_end(); ++it) { has_ai |= (*it)->is_ai_column() && !(*it)->is_unused(); }
    if (!has_ai) { continue; }
    std::string relation;
    if (OB_FAIL(column_relation(*table, guard, relation))) { break; }
    for (int batch = 0; OB_SUCC(ret) && batch < 32; ++batch) {
      bool found = false;
      if (OB_FAIL(backfill_column_row(proxy, *table, relation, found)) || !found) { break; }
    }
    if (OB_FAIL(ret)) { break; }
  }
  return ret;
}
} // namespace

int AiColumnWorker::start_workers(ObMySQLProxy &proxy)
{
  proxy_ = &proxy;
  set_thread_count(4);
  return start();
}

void AiColumnWorker::run1()
{
  int64_t last_backfill = 0;
  while (!has_set_stop()) {
    const auto now = ObTimeUtility::current_time();
    if (get_thread_idx() == 0 && now - last_backfill >= 1000000 && proxy_ != nullptr) {
      last_backfill = now;
      const int backfill_ret = backfill_column_tables(*proxy_);
      if (backfill_ret != OB_SUCCESS) { int ret = backfill_ret; LOG_WARN("AI column backfill deferred", K(ret)); }
    }
    bool found = false;
    const int ret = process_one(found);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) { LOG_WARN("AI column attempt deferred", K(ret)); }
    if (!found || ret != OB_SUCCESS) { usleep(100000); }
  }
}

int AiColumnWorker::process_one(bool &found)
{
  int ret = OB_SUCCESS;
  ColumnJob job;
  char owner[UuidCommon::LENGTH_UUID + 1] = {};
  found = false;
  if (proxy_ == nullptr) { return OB_NOT_INIT; }
  if (OB_FAIL(ObExprUuid::gen_server_uuid(owner, UuidCommon::LENGTH_UUID))) { return ret; }
  const std::string eligible = "((status='PENDING' AND eligible_ts<=now(6)) OR (status='RUNNING' AND lease_until<now(6)))";
  ObMySQLTransaction claim;
  if (OB_FAIL(claim.start(proxy_))) { return ret; }
  {
    ObMySQLProxy::MySQLResult result;
    const auto sql = "SELECT * FROM oceanbase.__all_ai_column_task WHERE " + eligible + " ORDER BY eligible_ts LIMIT 1 FOR UPDATE";
    if (OB_FAIL(claim.read(result, sql.c_str()))) {
    } else if (OB_FAIL(result.get_result()->next())) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    } else if (OB_FAIL(read_job(*result.get_result(), job))) {
    } else { found = true; }
  }
  int64_t claimed = 0;
  if (OB_SUCC(ret) && found) {
    const auto sql = "UPDATE oceanbase.__all_ai_column_task SET status='RUNNING',state=1,lease_owner=" + SqlText::literal(owner)
        + ",lease_epoch=lease_epoch+1,lease_until=DATE_ADD(now(6),INTERVAL 120 SECOND),attempts=attempts+1 WHERE "
        + job_filter(job) + " AND lease_epoch=" + std::to_string(job.epoch) + " AND " + eligible;
    ret = job_write(claim, sql, &claimed);
  }
  const int claim_end = claim.end(OB_SUCC(ret));
  if (OB_SUCC(ret)) { ret = claim_end; }
  if (OB_FAIL(ret)) { found = false; return ret; }
  if (!found) {
    // A process may stop after staging its last outcome but before publishing.
    // Terminal groups must therefore also be discoverable without a new attempt.
    ObMySQLProxy::MySQLResult result;
    const char *sql = "SELECT * FROM oceanbase.__all_ai_column_task WHERE status IN ('READY','FAILED') LIMIT 1";
    if (OB_FAIL(proxy_->read(result, sql))) { return ret; }
    if (OB_FAIL(result.get_result()->next())) { return ret == OB_ITER_END ? OB_SUCCESS : ret; }
    if (OB_FAIL(read_job(*result.get_result(), job))) { return ret; }
    result.reset();
    return publish_column_group(*proxy_, job);
  }
  if (claimed != 1) { found = false; return ret; }
  ++job.epoch;
  ++job.attempts;
  ObArenaAllocator allocator(ObMemAttr("AiColumnWorker"));
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObColumnSchemaV2 *column = nullptr;
  int outcome = OB_SUCCESS;
  if (GCTX.schema_service_ == nullptr) { outcome = OB_NOT_INIT; }
  else if (OB_SUCCESS != (outcome = GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_SUCCESS != (outcome = guard.get_table_schema(job.table_id, table))) {
  } else if (table == nullptr || table->is_in_recyclebin() || (column = table->get_column_schema(job.column_id)) == nullptr
      || !current_column_job(allocator, column, job)) { outcome = OB_SCHEMA_EAGAIN; }
  else if (job.attempts > job.max_retries + 1) { outcome = OB_TIMEOUT; }
  else if (!job.input_null) { outcome = execute_column_model(job, *column, allocator, job.output); }
  const bool retry = outcome != OB_SUCCESS && job.attempts <= job.max_retries;
  const auto filter = job_filter(job) + " AND status='RUNNING' AND lease_owner=" + SqlText::literal(owner)
      + " AND lease_epoch=" + std::to_string(job.epoch) + " AND lease_until>now(6)";
  const auto status = outcome == OB_SUCCESS ? "READY" : retry ? "PENDING" : "FAILED";
  const int state = outcome == OB_SUCCESS ? 2 : retry ? 3 : 4;
  const auto sql = "UPDATE oceanbase.__all_ai_column_task SET status='" + std::string(status) + "',state=" + std::to_string(state)
      + ",retry_count=" + std::to_string(outcome == OB_SUCCESS ? job.attempts - 1 : job.attempts)
      + ",eligible_ts=DATE_ADD(now(6),INTERVAL 1 SECOND),lease_owner=NULL,lease_until=NULL,result_value="
      + (outcome == OB_SUCCESS && !job.input_null ? SqlText::literal(job.output) : "NULL")
      + ",result_is_null=" + std::to_string(outcome == OB_SUCCESS && !job.input_null ? 0 : 1)
      + ",last_error=" + (outcome == OB_SUCCESS ? "NULL" : SqlText::literal(std::to_string(outcome))) + " WHERE " + filter;
  int64_t stored = 0;
  if (OB_FAIL(job_write(*proxy_, sql, &stored))) { return ret; }
  if (!retry && stored == 1) { ret = publish_column_group(*proxy_, job); }
  return ret;
}
} }
