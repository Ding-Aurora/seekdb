/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (!ob_is_string_or_lob_type(types_stack[CONTENT_IDX].get_type())) {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    }
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

static int call_embedding_group(ObIAllocator &allocator, ObString &model_id,
    ObArray<ObString> &contents, const int64_t dimension, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  auto *resolver = ::oceanbase::share::server_service<query::ObIAiEndpointResolver>();
  ObJsonObject *config = nullptr;
  ObJsonInt *dimension_node = nullptr;
  if (dimension > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimension, dimension_node))) {
    } else if (OB_FAIL(config->add("dimensions", dimension_node))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (model_id.empty() || resolver == nullptr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
  } else if (info == nullptr) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(resolver->resolve_by_model_name(model_id, allocator, endpoint))) {
  } else {
    ObAIFuncModel model(allocator, *info, endpoint);
    ret = model.call_dense_embedding_vector_v2(contents, config, results);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *model_arg = nullptr;
  ObDatum *content_arg = nullptr;
  ObDatum *dimension_arg = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, model_arg, content_arg, dimension_arg))) {
  } else if (model_arg->is_null() || content_arg->is_null()) {
    res.set_null();
  } else if (dimension_arg != nullptr && (dimension_arg->is_null() || dimension_arg->get_int() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension must be a positive integer");
  } else {
    ObEvalCtx::TempAllocGuard guard(ctx);
    MultimodeAlloctor allocator(guard.get_allocator());
    ObString model = model_arg->get_string();
    ObString content;
    ObArray<ObString> contents;
    ObArray<ObString> results;
    int64_t dimension = dimension_arg == nullptr ? 0 : dimension_arg->get_int();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *content_arg,
                   expr.args_[CONTENT_IDX]->datum_meta_, expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) {
    } else if (content.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(contents.push_back(content))) {
    } else if (OB_FAIL(call_embedding_group(allocator, model, contents, dimension, results))) {
    } else if (results.count() != 1) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, results.at(0)))) {
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
    const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
  ObDatum *output = expr.locate_batch_datums(ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ret = expr.args_[i]->eval_batch(ctx, skip, batch_size);
  }
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  MultimodeAlloctor allocator(alloc_guard.get_allocator());
  ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
  batch_guard.set_batch_size(batch_size);
  struct EmbeddingRow {
    int64_t index;
    ObString model;
    ObString content;
    int64_t dimension;
    bool done;
  };
  ObArray<EmbeddingRow> rows;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (skip.at(i) || evaluated.at(i)) {
      continue;
    }
    batch_guard.set_batch_idx(i);
    ObDatum &model = expr.args_[MODEL_IDX]->locate_expr_datum(ctx);
    ObDatum &content = expr.args_[CONTENT_IDX]->locate_expr_datum(ctx);
    ObDatum *dimension = expr.arg_cnt_ == 3 ? &expr.args_[DIM_IDX]->locate_expr_datum(ctx) : nullptr;
    if (model.is_null() || content.is_null()) {
      output[i].set_null();
      evaluated.set(i);
    } else if (dimension != nullptr && (dimension->is_null() || dimension->get_int() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      EmbeddingRow row{i, model.get_string(), ObString(), dimension == nullptr ? 0 : dimension->get_int(), false};
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, content,
                     expr.args_[CONTENT_IDX]->datum_meta_, expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), row.content))) {
      } else if (row.model.empty() || row.content.empty()) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(rows.push_back(row))) {
      }
    }
  }
  // Each model/dimension group uses one multi-input request. Keep explicit row
  // indexes because skipped and NULL rows are absent from the request.
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
    if (rows.at(i).done) {
      continue;
    }
    ObArray<ObString> contents;
    ObArray<int64_t> positions;
    ObArray<ObString> results;
    for (int64_t j = i; OB_SUCC(ret) && j < rows.count(); ++j) {
      if (!rows.at(j).done && rows.at(j).model == rows.at(i).model
          && rows.at(j).dimension == rows.at(i).dimension) {
        if (OB_FAIL(contents.push_back(rows.at(j).content))) {
        } else if (OB_FAIL(positions.push_back(j))) {
        } else {
          rows.at(j).done = true;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(call_embedding_group(allocator, rows.at(i).model, contents, rows.at(i).dimension, results))) {
    } else if (results.count() != positions.count()) {
      ret = OB_INVALID_DATA;
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < positions.count(); ++j) {
        int64_t index = rows.at(positions.at(j)).index;
        batch_guard.set_batch_idx(index);
        if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, output[index], results.at(j)))) {
        } else {
          evaluated.set(index);
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
