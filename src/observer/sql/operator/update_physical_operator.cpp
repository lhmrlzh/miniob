#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
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

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();

  // std::vector<Record> insert_records;
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();

    // insert_records.emplace_back(record);
    RC rc = RC::SUCCESS;
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }

    const std::vector<FieldMeta> *table_field_metas = table_->table_meta().field_metas();
    const char                   *target_field_name = field_.field_name();

    int meta_num     = table_field_metas->size();
    int target_index = -1;
    for (int i = 0; i < meta_num; ++i) {
      FieldMeta   fieldmeta  = (*table_field_metas)[i];
      const char *field_name = fieldmeta.name();
      if (0 == strcmp(field_name, target_field_name)) {
        target_index = i;
        break;
      }
    }

    // 初始化替换值
    std::vector<Value> values;
    int                cell_num = row_tuple->cell_num();

    Value value;
    for (int i = 0; i < cell_num; i++) {
      if (i == target_index) {
        values.push_back(value_);
      } else {
        rc = row_tuple->cell_at(i, value);
        values.push_back(value);
      }
    }

    // 删除记录
    rc = trx_->delete_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to remove record: %s", strrc(rc));
      return rc;
    }

    // 尝试创建记录
    rc = table_->make_record(static_cast<int>(values.size()), values.data(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }

    //插入记录
    rc = trx_->insert_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
