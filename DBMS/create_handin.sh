#!/bin/bash

[ -d handin ] || mkdir handin


cp src/sql/execution/execute.rs handin/
cp src/sql/execution/transform.rs handin/
cp src/sql/engine/local.rs handin/
cp src/sql/execution/join.rs handin/
cp src/sql/execution/write.rs handin/
cp src/sql/execution/aggregate.rs handin/
cp src/storage/tuple/row.rs handin/
cp src/storage/page/table_page/table_page.rs  handin/
cp src/storage/buffer/buffer_pool_manager/buffer_pool_manager.rs handin/
cp src/storage/buffer/lru_k_replacer/lru_k_replacer.rs handin/
cp src/sql/execution/source.rs handin/
