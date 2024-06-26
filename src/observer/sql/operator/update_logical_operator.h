#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief 更新逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, Field field, Value value);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }

  Table *table() { return table_; }
  Field  field() { return field_; }
  Value  value() { return value_; }

private:
  Table *table_ = nullptr;
  Field  field_;
  Value  value_;
};
