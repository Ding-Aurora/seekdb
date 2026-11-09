// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "share/schema/ob_table_schema.h"
#include "common/mysqlclient/ob_isql_client.h"

namespace oceanbase { namespace share { namespace schema {
class AiColumnSchemaUtils {
public:
  static int complete_table(ObTableSchema &table);
  static int replace_expression(ObColumnSchemaV2 &column, const common::ObString &definition);
  static int remap_columns(ObTableSchema &table, const common::hash::ObHashMap<uint64_t, uint64_t> &ids);
  static int persist_definition(common::ObISQLClient &client, const ObColumnSchemaV2 &column, bool replace = false);
  static int erase_definition(common::ObISQLClient &client, uint64_t table_id, uint64_t column_id = 0);
};
} } }
