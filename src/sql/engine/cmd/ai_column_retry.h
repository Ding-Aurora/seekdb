// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once
namespace oceanbase { namespace sql {
class ObExecContext;
class ObAlterTableStmt;
class AiColumnRetry {
public:
  static int execute(ObExecContext &context, ObAlterTableStmt &statement);
};
} }
