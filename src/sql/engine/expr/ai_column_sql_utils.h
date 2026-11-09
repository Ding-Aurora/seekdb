// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "lib/string/ob_string.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_tree.h"
#include "share/schema/ob_table_schema.h"
#include <string>

namespace oceanbase { namespace sql {
class AiColumnSqlUtils {
public:
  static std::string text(common::ObString value) { return std::string(value.ptr() == nullptr ? "" : value.ptr(), value.length()); }
  static std::string literal(const std::string &value)
  {
    static const char digits[] = "0123456789abcdef";
    std::string result = "X'";
    for (unsigned char c : value) { result += digits[c >> 4]; result += digits[c & 15]; }
    result += "'";
    return result;
  }
  static std::string identifier(const std::string &value)
  {
    std::string result = "`";
    for (char c : value) { result += c; if (c == '`') { result += c; } }
    return result + "`";
  }
  static std::string field(common::ObJsonObject &object, const char *name)
  {
    const auto *value = object.get_value(name);
    return value == nullptr ? "" : std::string(value->get_data(), value->get_data_length());
  }
  static std::string result_spec(common::ObJsonObject &definition)
  {
    std::string result = "{";
    for (const char *key : {"definition_version", "data_type", "dimension"}) {
      const auto *value = definition.get_value(key);
      if (value != nullptr) {
        if (result.size() != 1) { result += ","; }
        result += "\"" + std::string(key) + "\":" + std::to_string(value->get_int());
      }
    }
    return result + "}";
  }
  static std::string hidden(const share::schema::ObTableSchema &table, common::ObJsonObject &definition, const char *key)
  {
    const auto *id = definition.get_value(key);
    const auto *column = id == nullptr ? nullptr : table.get_column_schema(id->get_int());
    return column != nullptr && column->is_hidden() && !column->is_unused()
        ? identifier(text(column->get_column_name_str())) : std::string();
  }
  static int parse(common::ObIAllocator &allocator, common::ObString json, common::ObJsonObject *&object)
  {
    common::ObIJsonBase *base = nullptr;
    int ret = common::ObJsonBaseFactory::get_json_base(&allocator, json,
        common::ObJsonInType::JSON_TREE, common::ObJsonInType::JSON_TREE, base);
    if (ret == common::OB_SUCCESS) {
      if (base == nullptr || base->json_type() != common::ObJsonNodeType::J_OBJECT) { ret = common::OB_INVALID_DATA; }
      else { object = static_cast<common::ObJsonObject *>(base); }
    }
    return ret;
  }
};
} }
