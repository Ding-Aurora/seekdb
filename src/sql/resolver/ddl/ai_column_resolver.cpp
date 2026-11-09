// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_RESV
#include "ob_ddl_resolver.h"
#include "common/json_type/ob_json_tree.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include <vector>
#include <set>
#include <string>
#include <cctype>
#include <cstdio>

namespace oceanbase { namespace sql {
using namespace common;
using namespace share::schema;

// Preserve SQL input text, including quoted commas, nested calls and comments.
static int split_ai_column_arguments(ObString call, bool no_backslash, std::vector<ObString> &args)
{
  int depth = 0;
  int64_t start = -1;
  char quote = 0;
  bool line_comment = false;
  bool block_comment = false;
  for (int64_t i = 0; i < call.length(); ++i) {
    const char c = call.ptr()[i];
    const char next = i + 1 < call.length() ? call.ptr()[i + 1] : 0;
    if (line_comment) { if (c == '\n' || c == '\r') { line_comment = false; } continue; }
    if (block_comment) { if (c == '*' && next == '/') { block_comment = false; ++i; } continue; }
    if (quote != 0) {
      if (c == '\\' && !no_backslash && quote != '`') { ++i; }
      else if (c == quote) { if (next == quote) { ++i; } else { quote = 0; } }
      continue;
    }
    if (c == '\'' || c == '"' || c == '`') { quote = c; }
    else if (c == '#') { line_comment = true; }
    else if (c == '-' && next == '-' && i + 2 < call.length() && isspace(static_cast<unsigned char>(call.ptr()[i + 2]))) { line_comment = true; ++i; }
    else if (c == '/' && next == '*') { block_comment = true; ++i; }
    else if (c == '(') { if (++depth == 1) { start = i + 1; } }
    else if ((c == ',' && depth == 1) || (c == ')' && depth == 1)) {
      if (start < 0 || start >= i) { return OB_INVALID_ARGUMENT; }
      args.emplace_back(i - start, call.ptr() + start);
      start = i + 1;
      if (c == ')') { --depth; }
    } else if (c == ')') { if (--depth < 0) { return OB_INVALID_ARGUMENT; } }
  }
  return depth == 0 && quote == 0 && !block_comment && args.size() >= 2 && args.size() <= 3
      ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

static int collect_ai_dependencies(const ParseNode *node, std::set<std::string> &names, int depth = 0)
{
  int ret = OB_SUCCESS;
  if (node == nullptr) { return ret; }
  if (depth > 128) { return OB_SIZE_OVERFLOW; }
  if (node->type_ == T_SELECT || node->type_ == T_USER_VARIABLE_IDENTIFIER || node->type_ == T_SYSTEM_VARIABLE) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN subqueries and session variables");
  } else if (node->type_ == T_COLUMN_REF && node->num_child_ == 3 && node->children_[2] != nullptr) {
    const auto *name = node->children_[2];
    names.emplace(name->str_value_, name->str_len_);
  } else {
    if (node->type_ == T_FUN_SYS && node->num_child_ == 2 && node->children_[0] != nullptr) {
      const auto *name = node->children_[0];
      const ObString function(name->str_len_, name->str_value_);
      if (function.length() >= 3 && strncasecmp(function.ptr(), "ai_", 3) == 0 && function.case_compare("ai_prompt") != 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested AI functions in AI COLUMN input");
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ret = collect_ai_dependencies(node->children_[i], names, depth + 1);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_ai_column_attribute(ObColumnSchemaV2 &column, const ParseNode &node)
{
  int ret = OB_SUCCESS;
  const ParseNode *call = node.children_[0];
  const ParseNode *arguments = nullptr;
  ObString function;
  ObString model;
  int64_t retries = 2;
  int64_t dimension = 0;
  ObJsonObject definition(allocator_);
  ObJsonBuffer encoded(allocator_);
  std::set<std::string> dependencies;
  std::vector<ObString> argument_sql;
  if (column.is_ai_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple AI COLUMN declarations on one column");
  } else if (call == nullptr || call->type_ != T_FUN_SYS || call->num_child_ != 2
      || call->children_[0] == nullptr || call->children_[1] == nullptr) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN requires AI_COMPLETE or AI_EMBED");
  } else {
    function.assign_ptr(call->children_[0]->str_value_, call->children_[0]->str_len_);
    arguments = call->children_[1];
    if ((function.case_compare("ai_complete") != 0 && function.case_compare("ai_embed") != 0)
        || arguments->num_child_ < 2 || arguments->num_child_ > 3) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN function or argument count");
    } else if (arguments->children_[0]->type_ != T_VARCHAR || arguments->children_[0]->str_len_ == 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      model.assign_ptr(arguments->children_[0]->str_value_, arguments->children_[0]->str_len_);
      ret = collect_ai_dependencies(arguments->children_[1], dependencies);
      if (OB_SUCC(ret) && function.case_compare("ai_complete") == 0 && arguments->num_child_ == 3) {
        ret = collect_ai_dependencies(arguments->children_[2], dependencies);
        const auto *config = arguments->children_[2];
        if (OB_SUCC(ret) && config->type_ == T_VARCHAR) {
          ObIJsonBase *json = nullptr;
          ret = ObJsonBaseFactory::get_json_base(allocator_, ObString(config->str_len_, config->str_value_),
              ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json);
          if (OB_SUCC(ret) && (json == nullptr || json->json_type() != ObJsonNodeType::J_OBJECT)) { ret = OB_INVALID_ARGUMENT; }
        } else if (OB_SUCC(ret) && (config->type_ == T_INT || config->type_ == T_NUMBER || config->type_ == T_BOOL)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }
  const ParseNode *options = node.children_[1];
  std::set<std::string> seen_options;
  for (int i = 0; OB_SUCC(ret) && options != nullptr && i < options->num_child_; ++i) {
    const auto *option = options->children_[i];
    if (option == nullptr || option->num_child_ != 2) { ret = OB_INVALID_ARGUMENT; break; }
    const auto *key_node = option->children_[0];
    const auto *value = option->children_[1];
    ObString key(key_node->str_len_, key_node->str_value_);
    std::string folded(key.ptr(), key.length());
    for (char &ch : folded) { ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch))); }
    if (!seen_options.insert(folded).second) { ret = OB_INVALID_ARGUMENT; }
    else if (key.case_compare("sync_mode") == 0) {
      if (value->type_ != T_VARCHAR || ObString(value->str_len_, value->str_value_).case_compare("async") != 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN supports sync_mode='async'");
      }
    } else if (key.case_compare("max_retries") == 0) {
      if (value->type_ != T_INT || value->value_ < 0 || value->value_ > 32) { ret = OB_INVALID_ARGUMENT; }
      else { retries = value->value_; }
    } else { ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "unknown AI COLUMN option"); }
  }
  const bool embedding = function.case_compare("ai_embed") == 0;
  if (OB_SUCC(ret)) {
    bool no_backslash = false;
    IS_NO_BACKSLASH_ESCAPES(session_info_->get_sql_mode(), no_backslash);
    ret = split_ai_column_arguments(ObString(call->str_len_, call->str_value_), no_backslash, argument_sql);
  }
  if (OB_SUCC(ret)) {
    ObAIFuncExprInfo *info = nullptr;
    if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator_, model, info))) {
    } else if (info == nullptr || (embedding ? info->type_ != share::EndpointType::DENSE_EMBEDDING
                                           : info->type_ != share::EndpointType::COMPLETION)) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCC(ret) && embedding) {
    if (!column.is_collection() || column.get_extended_type_info().count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_EMBED columns require VECTOR(n)");
    } else {
      const auto type = column.get_extended_type_info().at(0);
      std::string text(type.ptr(), type.length());
      if (sscanf(text.c_str(), "VECTOR(%ld)", &dimension) != 1 || dimension <= 0) { ret = OB_INVALID_ARGUMENT; }
      else if (arguments->num_child_ == 3 && (arguments->children_[2]->type_ != T_INT
          || arguments->children_[2]->value_ != dimension)) {
        ret = OB_ERR_INVALID_VECTOR_DIM;
        LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, static_cast<unsigned>(dimension),
                       static_cast<unsigned>(arguments->children_[2]->value_));
      }
    }
  } else if (OB_SUCC(ret) && column.is_collection()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_COMPLETE result cannot be a vector");
  }
  auto text_field = [&](const char *key, ObString value) {
    if (OB_SUCC(ret)) {
      auto *text = OB_NEWx(ObJsonString, allocator_, value);
      ret = text == nullptr ? OB_ALLOCATE_MEMORY_FAILED : definition.add(key, text);
    }
  };
  auto number_field = [&](const char *key, int64_t value) {
    if (OB_SUCC(ret)) {
      auto *number = OB_NEWx(ObJsonInt, allocator_, value);
      ret = number == nullptr ? OB_ALLOCATE_MEMORY_FAILED : definition.add(key, number);
    }
  };
  if (OB_SUCC(ret)) {
    text_field("expression", ObString(call->str_len_, call->str_value_));
    text_field("input_expr", argument_sql.at(1));
    text_field("config_expr", !embedding && argument_sql.size() == 3 ? argument_sql.at(2) : ObString("NULL"));
    text_field("model", model);
    text_field("function", embedding ? ObString("AI_EMBED") : ObString("AI_COMPLETE"));
    text_field("sync_mode", ObString("async"));
    number_field("max_retries", retries);
    number_field("dimension", dimension);
    number_field("sql_mode", session_info_->get_sql_mode());
    number_field("data_type", column.get_data_type());
    const char *hint = column.is_json() ? "Return exactly one valid JSON value."
        : ob_is_integer_type(column.get_data_type()) ? "Return exactly one base-10 integer."
        : "Return only the requested value, without commentary or markdown fences.";
    text_field("type_prompt", ObString(hint));
    auto *names = OB_NEWx(ObJsonArray, allocator_, allocator_);
    if (names == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    for (const auto &name : dependencies) {
      if (OB_SUCC(ret)) {
        auto *text = OB_NEWx(ObJsonString, allocator_, ObString(name.size(), name.data()));
        ret = text == nullptr ? OB_ALLOCATE_MEMORY_FAILED : names->array_append(text);
      }
    }
    if (OB_SUCC(ret)) { ret = definition.add("dependencies", names); }
    if (OB_SUCC(ret)) { ret = definition.print(encoded, 0); }
    if (OB_SUCC(ret)) { ret = column.set_ai_column_definition(ObString(encoded.length(), encoded.ptr())); }
  }
  return ret;
}
} }
