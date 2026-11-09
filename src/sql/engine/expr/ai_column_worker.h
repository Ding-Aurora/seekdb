// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "lib/thread/thread_pool.h"
#include "common/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase { namespace sql {
class AiColumnWorker : public lib::ThreadPool {
public:
  int start_workers(common::ObMySQLProxy &proxy);
  void run1() override;
private:
  int process_one(bool &found);
  common::ObMySQLProxy *proxy_ = nullptr;
};
} }
