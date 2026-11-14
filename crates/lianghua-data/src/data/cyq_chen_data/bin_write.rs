use duckdb::{
    Connection,
    arrow::{
        array::{Array, Float64Array, Int32Array, StringArray},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    collections::HashMap,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

static BATCHES: LazyLock<Mutex<HashMap<u64, RecordBatch>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);

struct BatchRegistration(u64);
impl Drop for BatchRegistration {
    fn drop(&mut self) {
        BATCHES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}

pub(super) struct BinScan;
impl VTab for BinScan {
    type BindData = RecordBatch;
    type InitData = AtomicUsize;

    fn bind(info: &BindInfo) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let batch = BATCHES
            .lock()
            .map_err(|_| "筹码分桶导入注册表锁异常")?
            .get(&info.get_parameter(0).to_uint64())
            .cloned()
            .ok_or("筹码分桶导入批次不存在")?;
        for field in batch.schema().fields() {
            let logical = match field.data_type() {
                DataType::Utf8 => LogicalTypeId::Varchar,
                DataType::Int32 => LogicalTypeId::Integer,
                DataType::Float64 => LogicalTypeId::Double,
                other => return Err(format!("筹码分桶导入不支持 {other}").into()),
            };
            info.add_result_column(field.name(), logical.into());
        }
        Ok(batch)
    }

    fn init(_: &InitInfo) -> Result<AtomicUsize, Box<dyn std::error::Error>> {
        Ok(AtomicUsize::new(0))
    }

    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let batch = info.get_bind_data();
        let capacity = output.flat_vector(0).capacity();
        let start = info.get_init_data().fetch_add(capacity, Ordering::Relaxed);
        if start >= batch.num_rows() {
            output.set_len(0);
            return Ok(());
        }
        let len = capacity.min(batch.num_rows() - start);
        for (column, array) in batch.columns().iter().enumerate() {
            let mut vector = output.flat_vector(column);
            match array.data_type() {
                DataType::Utf8 => {
                    let strings = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("分桶字符串列类型错误")?;
                    // The vector's fixed-capacity string slots stay in place; long-string
                    // insertion only changes the auxiliary heap, not this slot allocation.
                    let slots = unsafe { vector.as_mut_ptr::<duckdb::ffi::duckdb_string_t>() };
                    for row in 0..len {
                        if strings.is_null(start + row) {
                            vector.set_null(row);
                            continue;
                        }
                        let value = strings.value(start + row);
                        if value.len() <= 12 {
                            let mut inline =
                                duckdb::ffi::duckdb_string_t__bindgen_ty_1__bindgen_ty_2 {
                                    length: value.len() as u32,
                                    inlined: [0; 12],
                                };
                            for (target, byte) in inline.inlined.iter_mut().zip(value.bytes()) {
                                *target = byte as std::os::raw::c_char;
                            }
                            // VARCHAR's public C representation stores <=12 UTF-8 bytes inline.
                            // The vector has at least `len` slots and is exclusively written here;
                            // no pointers to Arrow buffers escape this synchronous scan.
                            unsafe {
                                slots.add(row).write(duckdb::ffi::duckdb_string_t {
                                    value: duckdb::ffi::duckdb_string_t__bindgen_ty_1 {
                                        inlined: inline,
                                    },
                                });
                            }
                        } else {
                            vector.insert(row, value);
                        }
                    }
                }
                DataType::Int32 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("分桶整数列类型错误")?;
                    // Logical INTEGER is physically i32; every selected slot is initialized.
                    unsafe {
                        vector.as_mut_slice::<i32>()[..len]
                            .copy_from_slice(&values.values()[start..start + len]);
                    }
                    for row in 0..len {
                        if values.is_null(start + row) {
                            vector.set_null(row);
                        }
                    }
                }
                DataType::Float64 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or("分桶数值列类型错误")?;
                    // Logical DOUBLE is physically f64; every selected slot is initialized.
                    unsafe {
                        vector.as_mut_slice::<f64>()[..len]
                            .copy_from_slice(&values.values()[start..start + len]);
                    }
                    for row in 0..len {
                        if values.is_null(start + row) {
                            vector.set_null(row);
                        }
                    }
                }
                _ => return Err("分桶导入列类型已改变".into()),
            }
        }
        output.set_len(len);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::UBigint.into()])
    }
}

pub(super) fn append_bin_batch(conn: &Connection, batch: RecordBatch) -> Result<(), String> {
    // fetch_update is available on the supported Rust 1.89; its newer alias is not.
    #[allow(deprecated)]
    let token = NEXT_BATCH
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| "筹码分桶导入批次编号耗尽")?;
    let registration = BatchRegistration(token);
    BATCHES
        .lock()
        .map_err(|_| "筹码分桶导入注册表锁异常")?
        .insert(token, batch);
    let result = conn
        .execute(
            "INSERT INTO cyq_chen_bin SELECT * FROM cyq_chen_bin_input(?)",
            [token],
        )
        .map(|_| ())
        .map_err(|e| format!("批量导入cyq_chen_bin失败:{e}"));
    drop(registration);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::arrow::{
        array::ArrayRef,
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;

    #[test]
    fn inline_import_matches_arrow_for_slices_nulls_and_long_utf8() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE cyq_chen_bin(s VARCHAR,n INTEGER,v DOUBLE); CREATE TABLE expected AS SELECT * FROM cyq_chen_bin").unwrap();
        conn.register_table_function::<BinScan>("cyq_chen_bin_input")
            .unwrap();
        let text = StringArray::from_iter((0..5010).map(|i| match i % 6 {
            0 => None,
            1 => Some(""),
            2 => Some("000001.SZ"),
            3 => Some("一二三四"),
            4 => Some("longer than twelve bytes"),
            _ => Some("20261115"),
        }));
        let nums = Int32Array::from_iter((0..5010).map(|i| (i % 11 != 0).then_some(-i)));
        let vals = Float64Array::from_iter(
            (0..5010)
                .map(|i| (i % 7 != 0).then_some(if i % 3 == 0 { -0.0 } else { i as f64 * 0.123 })),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
            Field::new("v", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(text) as ArrayRef, Arc::new(nums), Arc::new(vals)],
        )
        .unwrap()
        .slice(3, 5003);
        {
            let mut app = conn.appender("expected").unwrap();
            app.append_record_batch(batch.clone()).unwrap();
            app.flush().unwrap();
        }
        conn.execute_batch("BEGIN").unwrap();
        append_bin_batch(&conn, batch.clone()).unwrap();
        let different:usize = conn.query_row("SELECT count(*) FROM ((SELECT * FROM expected EXCEPT ALL SELECT * FROM cyq_chen_bin) UNION ALL (SELECT * FROM cyq_chen_bin EXCEPT ALL SELECT * FROM expected))",[],|r|r.get(0)).unwrap();
        assert_eq!(different, 0);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM cyq_chen_bin", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            5003
        );
        conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM cyq_chen_bin", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            0
        );
        conn.execute_batch("DROP TABLE cyq_chen_bin").unwrap();
        assert!(append_bin_batch(&conn, batch).is_err());
    }
}
