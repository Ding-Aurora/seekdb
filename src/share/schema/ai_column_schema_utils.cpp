// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ai_column_schema_utils.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_tree.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include <vector>

namespace oceanbase { namespace share { namespace schema {
using namespace common;

static int definition_tree(ObIAllocator &allocator, const ObString &json, ObJsonObject *&object)
{
  ObIJsonBase *base = nullptr;
  int ret = ObJsonBaseFactory::get_json_base(&allocator, json, ObJsonInType::JSON_TREE,
                                            ObJsonInType::JSON_TREE, base);
  if (OB_SUCC(ret)) {
    if (base == nullptr || base->json_type() != ObJsonNodeType::J_OBJECT) { ret = OB_INVALID_DATA; }
    else { object = static_cast<ObJsonObject *>(base); }
  }
  return ret;
}

static constexpr const char *ai_hidden_keys[] = {"state_column_id", "request_column_id", "result_spec_column_id"};

static int save_ai_definition(ObIAllocator &allocator, ObJsonObject &definition, ObColumnSchemaV2 &column)
{
  ObJsonBuffer encoded(&allocator);
  int ret = definition.print(encoded, 0);
  if (OB_SUCC(ret)) { ret = column.set_ai_column_definition(ObString(encoded.length(), encoded.ptr())); }
  return ret;
}

int AiColumnSchemaUtils::replace_expression(ObColumnSchemaV2 &column, const ObString &expression)
{
  if (!column.is_ai_column()) { return column.set_ai_column_definition(expression); }
  ObArenaAllocator allocator(ObMemAttr("AiColAlter"));
  ObJsonObject *previous = nullptr;
  ObJsonObject *replacement = nullptr;
  int ret = OB_SUCCESS;
  if (OB_FAIL(definition_tree(allocator, column.get_ai_column_definition(), previous))) { return ret; }
  if (OB_FAIL(definition_tree(allocator, expression, replacement))) { return ret; }
  for (const char *key : ai_hidden_keys) {
    const auto *old_id = previous->get_value(key);
    if (old_id == nullptr) { continue; }
    auto *id = OB_NEWx(ObJsonInt, &allocator, old_id->get_int());
    if (id == nullptr) { return OB_ALLOCATE_MEMORY_FAILED; }
    if (OB_FAIL(replacement->add(key, id))) { return ret; }
  }
  return save_ai_definition(allocator, *replacement, column);
}

int AiColumnSchemaUtils::remap_columns(ObTableSchema &table, const hash::ObHashMap<uint64_t, uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr("AiColRemap"));
  for (auto it = table.column_begin(); OB_SUCC(ret) && it != table.column_end(); ++it) {
    if (!(*it)->is_ai_column() || (*it)->is_unused()) { continue; }
    ObJsonObject *definition = nullptr;
    if (OB_FAIL(definition_tree(allocator, (*it)->get_ai_column_definition(), definition))) { break; }
    for (const char *key : ai_hidden_keys) {
      const auto *value = definition->get_value(key);
      uint64_t id = 0;
      if (value == nullptr) { ret = OB_INVALID_DATA; break; }
      if (OB_FAIL(ids.get_refactored(value->get_int(), id))) { break; }
      auto *number = OB_NEWx(ObJsonInt, &allocator, id);
      if (number == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
      if (OB_FAIL(definition->add(key, number))) { break; }
    }
    if (OB_FAIL(ret)) { break; }
    auto *prior = definition->get_value("dependency_ids");
    auto *dependencies = OB_NEWx(ObJsonArray, &allocator, &allocator);
    if (prior == nullptr || dependencies == nullptr) { ret = OB_INVALID_DATA; break; }
    for (int64_t i = 0; OB_SUCC(ret) && i < prior->element_count(); ++i) {
      uint64_t id = 0;
      if (OB_FAIL(ids.get_refactored(prior->get_value(i)->get_int(), id))) { break; }
      auto *number = OB_NEWx(ObJsonInt, &allocator, id);
      ret = number == nullptr ? OB_ALLOCATE_MEMORY_FAILED : dependencies->array_append(number);
    }
    if (OB_SUCC(ret) && OB_FAIL(definition->add("dependency_ids", dependencies))) {
    } else if (OB_SUCC(ret)) { ret = save_ai_definition(allocator, *definition, **it); }
  }
  return ret;
}

int AiColumnSchemaUtils::complete_table(ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  std::vector<uint64_t> columns;
  uint64_t next_id = OB_APP_MIN_COLUMN_ID;
  const uint64_t max_used_id = table.get_max_used_column_id();
  if (max_used_id != OB_INVALID_ID && max_used_id >= next_id) { next_id = max_used_id + 1; }
  for (auto it = table.column_begin(); it != table.column_end(); ++it) {
    if ((*it)->get_column_id() >= next_id) { next_id = (*it)->get_column_id() + 1; }
    if ((*it)->is_ai_column() && !(*it)->is_unused()) { columns.push_back((*it)->get_column_id()); }
  }
  ObArenaAllocator allocator(ObMemAttr("AiColumnSchema"));
  for (auto id : columns) {
    if (OB_FAIL(ret)) { break; }
    const auto *original = table.get_column_schema(id);
    if (!original->is_nullable() || original->get_rowkey_position() != 0 || original->is_generated_column()
        || original->is_autoincrement() || !original->get_cur_default_value().is_null()) {
      ret = OB_NOT_SUPPORTED;
      break;
    }
    ObJsonObject *definition = nullptr;
    if (OB_FAIL(definition_tree(allocator, original->get_ai_column_definition(), definition))) { break; }
    if (definition->get_value("definition_version") != nullptr) { continue; }
    auto *version = OB_NEWx(ObJsonInt, &allocator, ObTimeUtility::current_time());
    if (version == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
    if (OB_FAIL(definition->add("definition_version", version))) { break; }
    auto *dependencies = definition->get_value("dependencies");
    if (dependencies == nullptr || dependencies->json_type() != ObJsonNodeType::J_ARRAY) { ret = OB_INVALID_DATA; break; }
    auto *dependency_ids = OB_NEWx(ObJsonArray, &allocator, &allocator);
    if (dependency_ids == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
    for (int64_t i = 0; OB_SUCC(ret) && i < dependencies->element_count(); ++i) {
      auto *name = dependencies->get_value(i);
      const auto *dependency = table.get_column_schema(ObString(name->get_data_length(), name->get_data()));
      if (dependency == nullptr) { ret = OB_ERR_BAD_FIELD_ERROR; }
      else if (dependency->is_ai_column()) {
        ret = OB_NOT_SUPPORTED;
      } else {
        auto *value = OB_NEWx(ObJsonInt, &allocator, dependency->get_column_id());
        ret = value == nullptr ? OB_ALLOCATE_MEMORY_FAILED : dependency_ids->array_append(value);
      }
    }
    if (OB_SUCC(ret)) { ret = definition->add("dependency_ids", dependency_ids); }
    const char *prefixes[] = {"__ai_state_", "__ai_request_", "__ai_result_spec_"};
    for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
      ObSqlString name;
      if (OB_FAIL(name.append_fmt("%s%lu", prefixes[i], id))) { break; }
      const auto *bound = definition->get_value(ai_hidden_keys[i]);
      const auto *existing = bound == nullptr ? table.get_column_schema(name.string()) : table.get_column_schema(bound->get_int());
      if (existing != nullptr && existing->is_hidden() && !existing->is_unused()) {
        auto *number = OB_NEWx(ObJsonInt, &allocator, existing->get_column_id());
        ret = number == nullptr ? OB_ALLOCATE_MEMORY_FAILED : definition->add(ai_hidden_keys[i], number);
        continue;
      }
      ObColumnSchemaV2 hidden;
      hidden.set_table_id(table.get_table_id());
      hidden.set_column_id(next_id++);
      hidden.set_schema_version(original->get_schema_version());
      hidden.set_is_hidden(true);
      hidden.set_nullable(i != 0);
      hidden.set_data_type(i == 0 ? ObIntType : ObVarcharType);
      hidden.set_collation_type(i == 0 ? CS_TYPE_BINARY : CS_TYPE_UTF8MB4_BIN);
      hidden.set_charset_type(i == 0 ? CHARSET_BINARY : CHARSET_UTF8MB4);
      if (i != 0) { hidden.set_data_length(i == 1 ? 64 : 4096); }
      ObObj value;
      if (i == 0) { value.set_int(0); } else { value.set_null(); }
      if (OB_FAIL(hidden.set_column_name(name.string()))) {
      } else if (OB_FAIL(hidden.set_orig_default_value(value))) {
      } else if (OB_FAIL(hidden.set_cur_default_value(value, false))) {
      } else if (OB_FAIL(table.add_column(hidden))) {
      } else {
        auto *number = OB_NEWx(ObJsonInt, &allocator, hidden.get_column_id());
        ret = number == nullptr ? OB_ALLOCATE_MEMORY_FAILED : definition->add(ai_hidden_keys[i], number);
      }
    }
    if (OB_SUCC(ret)) { ret = save_ai_definition(allocator, *definition, *table.get_column_schema(id)); }
  }
  if (OB_SUCC(ret) && !columns.empty()) { table.set_max_used_column_id(next_id - 1); }
  return ret;
}

int AiColumnSchemaUtils::persist_definition(ObISQLClient &client, const ObColumnSchemaV2 &column, bool replace)
{
  if (!column.is_ai_column()) { return OB_SUCCESS; }
  ObArenaAllocator allocator(ObMemAttr("AiColumnMeta"));
  ObJsonObject *definition = nullptr;
  int ret = definition_tree(allocator, column.get_ai_column_definition(), definition);
  ObDMLSqlSplicer dml;
  if (OB_SUCC(ret)) { ret = dml.add_pk_column("table_id", column.get_table_id()); }
  if (OB_SUCC(ret)) { ret = dml.add_pk_column("column_id", column.get_column_id()); }
  if (OB_SUCC(ret)) { ret = dml.add_column("column_name", ObHexEscapeSqlStr(column.get_column_name_str())); }
  if (OB_SUCC(ret)) {
    const auto *version = definition->get_value("definition_version");
    ret = version == nullptr ? OB_INVALID_DATA : dml.add_column("definition_version", version->get_int());
  }
  for (auto key : {"state_column_id", "request_column_id", "result_spec_column_id", "max_retries"}) {
    auto *value = definition == nullptr ? nullptr : definition->get_value(key);
    if (OB_SUCC(ret)) { ret = value == nullptr ? OB_INVALID_DATA : dml.add_column(key, value->get_int()); }
  }
  const char *sources[] = {"expression", "input_expr", "model", "function", "sync_mode"};
  const char *destinations[] = {"normalized_expr", "input_expr", "model_name", "function_kind", "sync_mode"};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    auto *value = definition->get_value(sources[i]);
    if (value == nullptr) { ret = OB_INVALID_DATA; }
    else if (i == 0) {
      auto *hint = definition->get_value("type_prompt");
      ObSqlString normalized;
      if (OB_FAIL(normalized.append_fmt("%.*s /* %.*s */", static_cast<int>(value->get_data_length()), value->get_data(),
              hint == nullptr ? 0 : static_cast<int>(hint->get_data_length()), hint == nullptr ? "" : hint->get_data()))) {
      } else { ret = dml.add_column(destinations[i], ObHexEscapeSqlStr(normalized.string())); }
    } else { ret = dml.add_column(destinations[i], ObHexEscapeSqlStr(ObString(value->get_data_length(), value->get_data()))); }
  }
  ObJsonBuffer dependencies(&allocator);
  if (OB_SUCC(ret)) { ret = definition->get_value("dependency_ids")->print(dependencies, 0); }
  if (OB_SUCC(ret)) { ret = dml.add_column("dependency_columns", ObHexEscapeSqlStr(ObString(dependencies.length(), dependencies.ptr()))); }
  if (OB_SUCC(ret)) { ret = dml.add_column("result_spec", ObHexEscapeSqlStr(column.get_ai_column_definition())); }
  if (OB_SUCC(ret)) {
    ObDMLExecHelper exec(client);
    int64_t affected = 0;
    ret = replace ? exec.exec_replace("__all_ai_column", dml, affected) : exec.exec_insert("__all_ai_column", dml, affected);
  }
  return ret;
}

int AiColumnSchemaUtils::erase_definition(ObISQLClient &client, uint64_t table_id, uint64_t column_id)
{
  if (is_inner_table(table_id)) { return OB_SUCCESS; }
  int ret = OB_SUCCESS;
  for (const char *name : {"__all_ai_column_task", "__all_ai_column"}) {
    ObSqlString sql;
    int64_t affected = 0;
    if (OB_FAIL(sql.append_fmt("DELETE FROM oceanbase.%s WHERE table_id=%lu", name, table_id))) {
    } else if (column_id != 0 && OB_FAIL(sql.append_fmt(" AND column_id=%lu", column_id))) {
    } else if (OB_FAIL(client.write(sql.ptr(), affected))) {
    }
    if (OB_FAIL(ret)) { break; }
  }
  return ret;
}
} } }
