use crate::{
    data::RowData,
    expr::eval::{Runtime, Value},
};

pub fn row_into_rt(row_data: RowData) -> Result<Runtime, String> {
    let mut runtime = Runtime::default();
    for (name, column) in row_data.cols {
        runtime.vars.insert(name, Value::NumSeries(column));
    }
    Ok(runtime)
}

