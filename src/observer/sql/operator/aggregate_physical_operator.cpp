#include "sql/operator/aggregate_physical_operator.h"

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation) { aggregations_.push_back(aggregation); }

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  if (result_tuple_.cell_num() > 0) {
    return RC::RECORD_EOF;
  }

  RC                rc   = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  std::vector<Value> result_cells;
  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();

    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];

      Value    cell;
      AttrType attr_type = AttrType::INTS;
      switch (aggregation) {
        case AggrOp::AGGR_SUM:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            result_cells.push_back(Value(0));
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;
        default: return RC::UNIMPLENMENT;
      }
    }
  }

  if (!result_cells.empty())
    result_cells.pop_back();
  LOG_TRACE("result_cells size: %d",result_cells.size());
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  result_tuple_.set_cells(result_cells);
  return rc;
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *AggregatePhysicalOperator::current_tuple() { return &result_tuple_; }