// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_RESV
#include "ob_dml_resolver.h"
#include "ob_select_resolver.h"
#include "sql/engine/basic/file_scan_utils.h"
#include "sql/ob_sql_utils.h"
#include "common/json_type/ob_json_tree.h"

namespace oceanbase { namespace sql {
using namespace common;

static int file_constant(const ParseNode *node, const ParamStore *parameters, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (node == nullptr) { ret = OB_INVALID_ARGUMENT; }
  else if (node->type_ == T_VARCHAR || node->type_ == T_CHAR || node->type_ == T_HEX_STRING) {
    value.set_varchar(ObString(node->str_len_, node->str_value_));
    value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else if (node->type_ == T_INT || node->type_ == T_BOOL) {
    value.set_int(node->value_);
  } else if (node->type_ == T_OP_NEG && node->num_child_ == 1
      && node->children_[0]->type_ == T_INT && node->children_[0]->value_ >= 0) {
    value.set_int(-node->children_[0]->value_);
  } else if (node->type_ == T_QUESTIONMARK && parameters != nullptr
      && node->value_ >= 0 && node->value_ < parameters->count()) {
    value = parameters->at(node->value_);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file paths and options require constant literals");
  }
  return ret;
}

int ObSelectResolver::resolve_file_copy(const ParseNode &node, ObSelectIntoItem &item)
{
  int ret = FileScanUtils::check_enabled(*session_info_);
  // File identity and schema must be inferred again for every statement.
  const_cast<ObGlobalHint &>(get_stmt()->get_query_ctx()->get_global_hint())
      .merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  ObJsonObject options(allocator_);
  ObJsonBuffer encoded(allocator_);
  if (OB_SUCC(ret)) { ret = file_constant(node.children_[0], params_.param_list_, item.outfile_name_); }
  const ParseNode *list = node.children_[1];
  for (int64_t i = 0; OB_SUCC(ret) && list != nullptr && i < list->num_child_; ++i) {
    const ParseNode *option = list->children_[i];
    ObString name(option->children_[0]->str_len_, option->children_[0]->str_value_);
    const ParseNode *value = option->children_[1];
    ObObj object;
    if (name.case_compare("format") != 0) { ret = file_constant(value, params_.param_list_, object); }
    if (OB_FAIL(ret)) {
    } else if (name.case_compare("format") == 0) {
      if (value->str_value_ == nullptr || ObString(value->str_len_, value->str_value_).case_compare("csv") != 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY supports FORMAT CSV");
      }
    } else if (name.case_compare("header") == 0) {
      if (!object.is_integer_type() || (object.get_int() != 0 && object.get_int() != 1)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        auto *boolean = OB_NEWx(ObJsonBoolean, allocator_, object.get_int() != 0);
        if (boolean == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
        else { ret = options.add("header", boolean); }
      }
    } else if (name.case_compare("delim") == 0 || name.case_compare("delimiter") == 0
        || name.case_compare("quote") == 0 || name.case_compare("escape") == 0 || name.case_compare("nullstr") == 0) {
      if (!object.is_string_type()) { ret = OB_INVALID_ARGUMENT; }
      else {
        const char *key = name.case_compare("quote") == 0 ? "quote" : name.case_compare("escape") == 0 ? "escape"
            : name.case_compare("nullstr") == 0 ? "nullstr" : "delim";
        auto *text = OB_NEWx(ObJsonString, allocator_, object.get_string());
        if (text == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
        else { ret = options.add(key, text); }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "unknown COPY option");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(options.print(encoded, 0))) {
  } else if (OB_SUCC(ret)) {
    ObString text;
    if (OB_FAIL(ob_write_string(*allocator_, ObString(encoded.length(), encoded.ptr()), text))) {
    } else {
      item.field_str_.set_varchar(text);
      item.field_str_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      const auto &items = get_select_stmt()->get_select_items();
      for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
        const SelectItem &column = items.at(i);
        ObString header;
        if (OB_FAIL(ob_write_string(*allocator_, column.alias_name_.empty() ? column.expr_name_ : column.alias_name_, header))) {
        } else { ret = item.user_vars_.push_back(header); }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_file_table_item(const ParseNode &parse_tree, TableItem *&tbl_item)
{
  int ret = FileScanUtils::check_enabled(*session_info_);
  const_cast<ObGlobalHint &>(get_stmt()->get_query_ctx()->get_global_hint())
      .merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  const ParseNode &call = *parse_tree.children_[0];
  const ParseNode *args = call.children_[1];
  ObString function(call.children_[0]->str_len_, call.children_[0]->str_value_);
  ObString path;
  ObString secure;
  ObJsonObject options(allocator_);
  ObJsonBuffer options_text(allocator_);
  if (OB_SUCC(ret) && parse_tree.value_ == 1) {
    ObJsonBoolean *enabled = OB_NEWx(ObJsonBoolean, allocator_, true);
    if (enabled == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { ret = options.add("__describe", enabled); }
  }
  const char *format = function.case_compare("read_csv") == 0 ? "csv"
      : function.case_compare("read_parquet") == 0 ? "parquet"
      : function.case_compare("glob") == 0 ? "glob" : "json";
  if (OB_FAIL(ret)) {
  } else if (args == nullptr || args->num_child_ < 1) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(FileScanUtils::secure_root(*allocator_, secure))) {
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < args->num_child_; ++index) {
    const ParseNode *node = args->children_[index];
    const ParseNode *value = node;
    ObString key;
    if (index != 0) {
      if (node == nullptr || node->type_ != T_OP_EQ || node->num_child_ != 2) {
        ret = OB_ERR_PARSE_SQL;
        LOG_USER_ERROR(OB_ERR_PARSE_SQL, "file options require name=value", 0, "", 1);
      } else {
        const ParseNode *identifier = node->children_[0];
        if (identifier != nullptr && identifier->type_ == T_COLUMN_REF && identifier->num_child_ == 3) {
          identifier = identifier->children_[2];
        }
        if (identifier == nullptr || identifier->str_value_ == nullptr) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          key.assign_ptr(identifier->str_value_, identifier->str_len_);
          value = node->children_[1];
        }
      }
    }
    ObObj object;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(file_constant(value, params_.param_list_, object))) {
    } else if (object.is_null()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (index == 0) {
      if (!object.is_string_type()) { ret = OB_INVALID_ARGUMENT; }
      else { path = object.get_string(); }
    } else {
      const bool is_csv = strcmp(format, "csv") == 0;
      if (key.case_compare("sample_size") == 0) {
        if (!object.is_integer_type() || object.get_int() != -1) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL requires full schema inference: sample_size=-1");
        }
      } else if (key.case_compare("format") == 0 && strcmp(format, "json") == 0) {
        if (!object.is_string_type() || (object.get_string().case_compare("newline_delimited") != 0
            && object.get_string().case_compare("auto") != 0)) { ret = OB_NOT_SUPPORTED; }
      } else if (is_csv && key.case_compare("header") == 0) {
        if (!object.is_integer_type() || (object.get_int() != 0 && object.get_int() != 1)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          ObJsonBoolean *boolean = OB_NEWx(ObJsonBoolean, allocator_, object.get_int() != 0);
          if (boolean == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
          else { ret = options.add("header", boolean); }
        }
      } else if (is_csv && (key.case_compare("delim") == 0 || key.case_compare("delimiter") == 0
                  || key.case_compare("sep") == 0 || key.case_compare("quote") == 0
                  || key.case_compare("escape") == 0 || key.case_compare("nullstr") == 0)) {
        if (!object.is_string_type()) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          const char *name = key.case_compare("quote") == 0 ? "quote" : key.case_compare("escape") == 0 ? "escape"
              : key.case_compare("nullstr") == 0 ? "nullstr" : "delim";
          ObJsonString *text = OB_NEWx(ObJsonString, allocator_, object.get_string());
          if (text == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
          else { ret = options.add(name, text); }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "unknown file reader option");
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(options.print(options_text, 0))) {
  } else if (OB_FAIL(FileScanUtils::check_utf8(path))) {
  } else if (OB_FAIL(FileScanUtils::check_utf8(ObString(options_text.length(), options_text.ptr())))) {
  } else {
    auto source = local::file_inspect(rust::Str(path.ptr(), path.length()), format,
        rust::Str(options_text.ptr(), options_text.length()), rust::Str(secure.ptr(), secure.length()));
    if (OB_FAIL(FileScanUtils::status(*source))) {
    } else {
      auto columns = source->file_columns();
      if (columns.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file has no inferable columns");
      } else {
        const ParseNode *alias = parse_tree.children_[1];
        ObString name = alias == nullptr ? function : ObString(alias->str_len_, alias->str_value_);
        TableItem *item = nullptr;
        if (OB_FAIL(create_unnest_table_item(item, name))) {
        } else {
          item->json_table_def_->table_type_ = OB_FILE_TABLE_TYPE;
          ObDmlJtColDef *root = nullptr;
          rust::String snapshot = source->file_snapshot();
          if (OB_FAIL(get_json_table_column_by_id(item->table_id_, root))) {
          } else if (OB_FAIL(ob_write_string(*allocator_,
                      ObString(snapshot.size(), snapshot.data()), root->col_base_info_.path_))) {
          } else {
            ObConstRawExpr *document = nullptr;
            ObObj document_value;
            document_value.set_varchar(path);
            document_value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_VARCHAR, document))) {
            } else {
              document->set_value(document_value);
              if (OB_FAIL(document->deduce_type(session_info_))) {
              } else if (OB_FAIL(item->json_table_def_->doc_exprs_.push_back(document))) {
              }
            }
          }
          for (int64_t index = 0; OB_SUCC(ret) && index < columns.size(); ++index) {
            ObDmlJtColDef *definition = OB_NEWx(ObDmlJtColDef, allocator_);
            ColumnItem *column = nullptr;
            ObString column_name;
            if (definition == nullptr) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(ob_write_string(*allocator_,
                        ObString(columns[index].name.size(), columns[index].name.data()), column_name))) {
            } else {
              definition->table_id_ = item->table_id_;
              auto &base = definition->col_base_info_;
              base.id_ = index + 1;
              base.parent_id_ = 0;
              base.col_type_ = COL_TYPE_VALUE;
              base.col_name_ = column_name;
              base.data_type_ = FileScanUtils::column_type(columns[index]);
              if (OB_FAIL(generate_json_table_output_column_item(item, base.data_type_, column_name,
                            OB_APP_MIN_COLUMN_ID + index, column))) {
              } else {
                column->col_idx_ = item->json_table_def_->all_cols_.count();
                base.output_column_idx_ = column->column_id_;
                if (!columns[index].nullable) { column->expr_->set_result_flag(NOT_NULL_FLAG); }
                if (OB_FAIL(item->json_table_def_->all_cols_.push_back(&base))) {
                } else if (OB_FAIL(root->regular_cols_.push_back(definition))) {
                }
              }
            }
          }
          tbl_item = item;
        }
      }
    }
  }
  return ret;
}
} }
