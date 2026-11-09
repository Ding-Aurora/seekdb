// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once

namespace oceanbase { namespace sql {
class ObTableModifyOp;
struct ObDMLBaseCtDef;
struct ObDMLModifyRowNode;

class AiColumnDml {
public:
  static bool needs_after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef);
  static int after_row(ObDMLModifyRowNode &row);
};
} }
