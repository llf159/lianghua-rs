use crate::{
    data::RowData,
    expr::eval::{Runtime, Value},
};
use std::sync::Arc;

/// Build an independent runtime without copying unused dates. Runtime clones share input columns.
pub fn row_to_shared_rt(row_data: &RowData) -> Runtime {
    columns_to_shared_rt(
        row_data
            .cols
            .iter()
            .map(|(name, column)| (name.clone(), column.clone())),
    )
}

pub(crate) fn columns_to_shared_rt(
    columns: impl IntoIterator<Item = (String, Vec<Option<f64>>)>,
) -> Runtime {
    let mut runtime = Runtime::default();
    for (name, column) in columns {
        runtime
            .vars
            .insert(name, Value::SharedNumSeries(Arc::new(column)));
    }
    runtime
}

pub fn row_into_rt(row_data: RowData) -> Result<Runtime, String> {
    let mut runtime = Runtime::default();
    for (name, column) in row_data.cols {
        runtime.vars.insert(name, Value::NumSeries(column));
    }
    Ok(runtime)
}
