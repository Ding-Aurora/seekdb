// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "local_bridge.h"
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
#include <memory>
#include <vector>

namespace oceanbase { namespace sql {
class ObJsonTableSpec;
class ObSelectIntoSpec;

class FileScanUtils {
public:
  static int check_enabled(const ObSQLSessionInfo &session);
  static int check_utf8(const common::ObString &text);
  static int secure_root(common::ObIAllocator &allocator, common::ObString &root);
  static int status(const local::FileSource &source);
  static int status(const local::FileExport &output);
  static common::ObDataType column_type(const local::FileColumn &column);
};

class FileSqlScan {
public:
  FileSqlScan(const ObJsonTableSpec &spec, ObExecContext &context, ObEvalCtx &eval);
  int open();
  int next(bool reuse = true);
  void begin_batch() { row_allocator_.reuse(); }
  bool batch_full() const { return row_allocator_.used() >= 64 * 1024 * 1024; }
  int rescan();
private:
  const ObJsonTableSpec &spec_;
  ObExecContext &context_;
  ObEvalCtx &eval_;
  std::unique_ptr<rust::Box<local::FileSource>> source_;
  common::ObArenaAllocator row_allocator_;
  std::vector<int32_t> columns_;
  std::vector<int64_t> outputs_;
};

class FileSqlExport {
public:
  FileSqlExport(const ObSelectIntoSpec &spec, ObExecContext &context, ObEvalCtx &eval);
  int open();
  int write();
  int finish();
private:
  const ObSelectIntoSpec &spec_;
  ObExecContext &context_;
  ObEvalCtx &eval_;
  std::unique_ptr<rust::Box<local::FileExport>> output_;
  common::ObArenaAllocator row_allocator_;
};
} }
