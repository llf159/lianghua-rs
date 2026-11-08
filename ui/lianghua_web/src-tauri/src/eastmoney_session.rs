// 保留原前端命令名，东财与其他来源共用登录会话实现。
use crate::data_source_session::{self, LoginSource};

#[tauri::command]
pub async fn open_eastmoney_verification(app: tauri::AppHandle) -> Result<(), String> {
    data_source_session::open_data_source_login(app, LoginSource::Eastmoney).await
}

#[tauri::command]
pub async fn use_eastmoney_verified_session(app: tauri::AppHandle) -> Result<(), String> {
    data_source_session::use_data_source_session(app, LoginSource::Eastmoney, None).await
}

#[tauri::command]
pub fn stop_eastmoney_verified_session() -> Result<(), String> {
    data_source_session::stop_data_source_session(LoginSource::Eastmoney)
}
