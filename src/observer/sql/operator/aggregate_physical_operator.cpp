#include "sql/operator/aggregate_physical_operator.h"
#include <cstring>

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
  int                cnt = 0;
  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();

    cnt++;
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];

      Value    cell;
      AttrType attr_type = AttrType::INTS;
      switch (aggregation) {
        case AggrOp::AGGR_COUNT:
        case AggrOp::AGGR_COUNT_ALL:
          if (result_cells.size() <= unsigned(cell_idx))
            result_cells.push_back(Value(0));
          result_cells[cell_idx].set_int(cnt);
          break;
        case AggrOp::AGGR_SUM:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            if (result_cells.size() <= unsigned(cell_idx))
              result_cells.push_back(Value(0));
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;
        case AggrOp::AGGR_AVG:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            if (result_cells.size() <= unsigned(cell_idx))
              result_cells.push_back(Value(0));
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() * (cnt - 1));
            result_cells[cell_idx].set_float((result_cells[cell_idx].get_float() + cell.get_float()) / cnt);
          }
          break;
        case AggrOp::AGGR_MAX:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (result_cells.size() <= unsigned(cell_idx))
            result_cells.push_back(Value(0));
          switch (attr_type) {
            case AttrType::INTS:
              result_cells[cell_idx].set_int(std::max(result_cells[cell_idx].get_int(), cell.get_int()));
              break;
            case AttrType::FLOATS:
              result_cells[cell_idx].set_float(std::max(result_cells[cell_idx].get_float(), cell.get_float()));
              break;
            case AttrType::CHARS: {
              if (cnt == 1)
                result_cells[cell_idx].set_string("");
              std::string max_string = std::max(result_cells[cell_idx].get_string(), cell.get_string());
              char       *max_str    = new char[max_string.length() + 1];
              std::strcpy(max_str, max_string.c_str());
              LOG_DEBUG("max_str: %s ", max_str);
              result_cells[cell_idx].set_string(max_str, strlen(max_str));
              break;
            }
            case AttrType::DATES: {
              if (cnt == 1)
                result_cells[cell_idx].set_date(0);
              result_cells[cell_idx].set_date(std::max(result_cells[cell_idx].get_date(), cell.get_date()));
              break;
            }
            default: return RC::UNIMPLENMENT;
          }
          break;
        case AggrOp::AGGR_MIN:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (result_cells.size() <= unsigned(cell_idx))
            result_cells.push_back(Value(INT32_MAX));
          switch (attr_type) {
            case AttrType::INTS:
              result_cells[cell_idx].set_int(std::min(result_cells[cell_idx].get_int(), cell.get_int()));
              break;
            case AttrType::FLOATS:
              result_cells[cell_idx].set_float(std::min(result_cells[cell_idx].get_float(), cell.get_float()));
              break;
            case AttrType::CHARS: {
              if (cnt == 1)
                result_cells[cell_idx].set_string(cell.get_string().c_str(), cell.get_string().length());
              std::string min_string = std::min(result_cells[cell_idx].get_string(), cell.get_string());
              char       *min_str    = new char[min_string.length() + 1];
              std::strcpy(min_str, min_string.c_str());
              LOG_DEBUG("min_str: %s ", min_str);
              result_cells[cell_idx].set_string(min_str, strlen(min_str));
              break;
            }
            case AttrType::DATES: {
              if (cnt == 1)
                result_cells[cell_idx].set_date(24867);
              result_cells[cell_idx].set_date(std::min(result_cells[cell_idx].get_date(), cell.get_date()));
              break;
            }
            default: return RC::UNIMPLENMENT;
          }
          break;
        default: return RC::UNIMPLENMENT;
      }
    }
  }

  // if (!result_cells.empty())
  //   result_cells.pop_back();
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