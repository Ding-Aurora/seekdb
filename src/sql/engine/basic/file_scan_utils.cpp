// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#define USING_LOG_PREFIX SQL_ENG
#include "file_scan_utils.h"
#include "ob_json_table_op.h"
#include "ob_select_into_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_server_struct.h"

namespace oceanbase { namespace sql {
using namespace common;

int FileScanUtils::check_enabled(const ObSQLSessionInfo &session)
{
  ObObj value;
  int ret = session.get_sys_variable_by_name(ObString("enable_file_sql"), value);
  if (OB_SUCC(ret) && !value.is_null() && value.get_int() != 0) {
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled; SET SESSION enable_file_sql=ON");
  }
  return ret;
}

int FileScanUtils::secure_root(ObIAllocator &allocator, ObString &root)
{
  int ret = OB_SUCCESS;
  if (GCTX.schema_service_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = share::schema::ObSchemaUtils::get_runtime_varchar_variable(
        *GCTX.schema_service_, share::SYS_VAR_SECURE_FILE_PRIV, allocator, root);
    if (OB_SUCC(ret)) { ret = check_utf8(root); }
  }
  return ret;
}

int FileScanUtils::check_utf8(const ObString &text)
{
  int64_t length = 0;
  int ret = text.empty() ? OB_SUCCESS : ObCharset::well_formed_len(
      CS_TYPE_UTF8MB4_BIN, text.ptr(), text.length(), length);
  if (OB_SUCC(ret) && !text.empty() && length != text.length()) { ret = OB_INVALID_ARGUMENT; }
  if (OB_FAIL(ret)) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file SQL requires valid UTF-8 text"); }
  return ret;
}

int FileScanUtils::status(const local::FileSource &source)
{
  int ret = OB_SUCCESS;
  const int32_t code = source.file_status();
  if (code == 100) {
    ret = OB_ITER_END;
  } else if (code != 0) {
    ret = code == -2 ? OB_NOT_SUPPORTED : code == -4 ? OB_ERR_NO_PRIVILEGE : OB_INVALID_ARGUMENT;
    rust::String error = source.file_error();
    if (ret == OB_NOT_SUPPORTED) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, error.c_str());
    } else if (ret == OB_INVALID_ARGUMENT) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.c_str());
    }
    LOG_WARN("file scan failed", K(ret), "detail", error.c_str());
  }
  return ret;
}

ObDataType FileScanUtils::column_type(const local::FileColumn &column)
{
  ObDataType type;
  ObObjType kind = (column.kind & 15) == 0 ? ObNullType : (column.kind & 15) == 1 ? ObIntType
      : (column.kind & 15) == 2 ? ObDoubleType : (column.kind & 15) == 3 ? ObTinyIntType
      : (column.kind & 15) == 5 ? ObDateType : (column.kind & 15) == 6 ? ObDateTimeType : ObVarcharType;
  type.set_obj_type(kind);
  type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[kind]);
  if (kind == ObDateTimeType) { type.set_scale(column.kind >> 4); }
  if (kind == ObVarcharType) {
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(OB_MAX_VARCHAR_LENGTH);
  }
  return type;
}

FileSqlScan::FileSqlScan(const ObJsonTableSpec &spec, ObExecContext &context, ObEvalCtx &eval)
    : spec_(spec), context_(context), eval_(eval), row_allocator_(ObMemAttr("FileSqlRow")) {}

int FileSqlScan::open()
{
  int ret = OB_SUCCESS;
  ObString secure;
  if (OB_FAIL(FileScanUtils::check_enabled(*context_.get_my_session()))) {
  } else if (OB_FAIL(FileScanUtils::secure_root(context_.get_allocator(), secure))) {
  } else if (spec_.cols_def_.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t index = 1; index < spec_.cols_def_.count(); ++index) {
      if (spec_.cols_def_.at(index)->output_column_idx_ >= 0) {
        columns_.push_back(index - 1);
        outputs_.push_back(spec_.cols_def_.at(index)->output_column_idx_);
      }
    }
    const ObString &snapshot = spec_.cols_def_.at(0)->path_;
    source_.reset(new rust::Box<local::FileSource>(local::file_resume(
        rust::Str(snapshot.ptr(), snapshot.length()), rust::Str(secure.ptr(), secure.length()),
        rust::Slice<const int32_t>(columns_.data(), columns_.size()))));
    ret = FileScanUtils::status(**source_);
  }
  return ret;
}

int FileSqlScan::next(bool reuse)
{
  int ret = OB_SUCCESS;
  if (!source_) {
    ret = OB_NOT_INIT;
  } else {
    (*source_)->file_next();
    if (OB_FAIL(FileScanUtils::status(**source_))) {
    } else {
      if (reuse) { row_allocator_.reuse(); }
      const auto row = (*source_)->file_row();
      if (!outputs_.empty() && row.size() != outputs_.size()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObEvalCtx &eval = eval_;
        const auto params = ObBasicSessionInfo::create_dtc_params(context_.get_my_session());
        ObCastCtx cast(&row_allocator_, &params, CM_NONE, CS_TYPE_UTF8MB4_BIN);
        ObSqlObjCastRuntime runtime(&context_);
        cast.runtime_ = &runtime;
        for (int64_t i = 0; OB_SUCC(ret) && i < outputs_.size(); ++i) {
          ObExpr *expression = spec_.column_exprs_.at(outputs_[i]);
          ObDatum &datum = expression->locate_datum_for_write(eval);
          if (row[i].is_null) {
            datum.set_null();
          } else {
            ObObj input;
            ObObj output;
            ObObj owned;
            input.set_varchar(ObString(row[i].text.size(), row[i].text.data()));
            input.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            if (OB_FAIL(ObObjCaster::to_type(expression->obj_meta_.get_type(),
                  expression->obj_meta_.get_collation_type(), cast, input, output))) {
            } else if (OB_FAIL(ob_write_obj(row_allocator_, output, owned))) {
            } else if (OB_FAIL(datum.from_obj(owned))) {
            }
          }
          expression->set_evaluated_flag(eval);
        }
      }
    }
  }
  return ret;
}

int FileSqlScan::rescan()
{
  if (!source_) { return OB_NOT_INIT; }
  (*source_)->file_rescan();
  return FileScanUtils::status(**source_);
}

int FileScanUtils::status(const local::FileExport &output)
{
  const int32_t code = output.copy_status();
  if (code == 0) { return OB_SUCCESS; }
  auto error = output.copy_error();
  const int ret = code == -4 ? OB_ERR_NO_PRIVILEGE : OB_INVALID_ARGUMENT;
  if (ret == OB_INVALID_ARGUMENT) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.c_str()); }
  LOG_WARN("COPY failed", K(ret), "detail", error.c_str());
  return ret;
}

FileSqlExport::FileSqlExport(const ObSelectIntoSpec &spec, ObExecContext &context, ObEvalCtx &eval)
    : spec_(spec), context_(context), eval_(eval), row_allocator_(ObMemAttr("FileCopyRow")) {}

int FileSqlExport::open()
{
  int ret = FileScanUtils::check_enabled(*context_.get_my_session());
  ObString secure;
  if (OB_SUCC(ret)) { ret = FileScanUtils::secure_root(context_.get_allocator(), secure); }
  if (OB_SUCC(ret)) {
    std::vector<local::FileCell> headers;
    for (const auto &name : spec_.user_vars_) {
      if (OB_FAIL(FileScanUtils::check_utf8(name))) { break; }
      headers.push_back(local::FileCell{rust::String(name.ptr(), name.length()), false});
    }
    const ObString path = spec_.outfile_name_.get_string();
    const ObString options = spec_.field_str_.get_string();
    if (OB_SUCC(ret)) { ret = FileScanUtils::check_utf8(path); }
    if (OB_SUCC(ret)) { ret = FileScanUtils::check_utf8(options); }
    if (OB_FAIL(ret)) { return ret; }
    output_.reset(new rust::Box<local::FileExport>(local::file_export(
        rust::Str(path.ptr(), path.length()), rust::Str(options.ptr(), options.length()),
        rust::Str(secure.ptr(), secure.length()), rust::Slice<const local::FileCell>(headers.data(), headers.size()))));
    ret = FileScanUtils::status(**output_);
  }
  return ret;
}

int FileSqlExport::write()
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  std::vector<local::FileCell> cells;
  const auto params = ObBasicSessionInfo::create_dtc_params(context_.get_my_session());
  ObCastCtx cast(&row_allocator_, &params, CM_NONE, CS_TYPE_UTF8MB4_BIN);
  ObSqlObjCastRuntime runtime(&context_);
  cast.runtime_ = &runtime;
  for (const auto *expr : spec_.select_exprs_) {
    if (OB_FAIL(ret)) { break; }
    ObDatum *datum = nullptr;
    ObObj input;
    ObObj text;
    if (OB_FAIL(expr->eval(eval_, datum))) {
    } else if (datum->is_null()) {
      cells.push_back(local::FileCell{rust::String(), true});
    } else if (OB_FAIL(datum->to_obj(input, expr->obj_meta_))) {
    } else if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, CS_TYPE_UTF8MB4_BIN, cast, input, text))) {
    } else {
      const ObString value = text.get_string();
      if (OB_SUCC(ret) && OB_SUCC(FileScanUtils::check_utf8(value))) {
        cells.push_back(local::FileCell{rust::String(value.ptr(), value.length()), false});
      } else { ret = OB_INVALID_ARGUMENT; }
    }
  }
  if (OB_SUCC(ret)) {
    (*output_)->copy_write(rust::Slice<const local::FileCell>(cells.data(), cells.size()));
    ret = FileScanUtils::status(**output_);
  }
  return ret;
}

int FileSqlExport::finish()
{
  (*output_)->copy_finish();
  return FileScanUtils::status(**output_);
}
} }
