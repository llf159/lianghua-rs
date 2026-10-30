//! The same isolated scoring comparison as the desktop research workbench.
use lianghua_app_strategy::{
    rank_comparison::{RankComparisonRequest, run_rank_comparison},
    research_archive::{
        ResearchArchiveLease, finish_research_archive, research_artifact_path,
        research_input_manifest, save_research_archive, try_research_execution,
    },
};
fn main() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("用法：rank_comparison <请求.json> <输出.json>".into());
    }
    let request: RankComparisonRequest =
        serde_json::from_reader(std::fs::File::open(&args[0]).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let _execution = try_research_execution()?;
    let source = request.source_path.clone();
    let _archive_access =
        lianghua_app_strategy::research_archive::research_archive_lock(&source, false)?;
    let journal = save_research_archive(
        &source,
        request.parent_run_id.clone(),
        request.research_question.clone(),
        serde_json::json!({"settings":request,"notes":request.research_notes,"input_manifest":research_input_manifest(&source)?}),
        serde_json::json!({}),
        "rank_comparison",
        "running",
        None,
        None,
    )?;
    let _lease = ResearchArchiveLease {
        source_path: source.clone(),
        run_id: journal.run_id.clone(),
    };
    let mut result = run_rank_comparison(
        request,
        &|phase| {
            eprintln!("{phase}");
            Ok(())
        },
        &|| false,
    );
    if let Ok(value) = &mut result {
        if let Some(path) = value.membership_staging.take() {
            std::fs::rename(
                path,
                research_artifact_path(&source, &journal.run_id, "members.db")?,
            )
            .map_err(|e| e.to_string())?;
        }
        value.membership_saved = true;
        value.archive_run_id = Some(journal.run_id.clone());
    }
    let payload = match &result {
        Ok(value) => serde_json::to_value(value).map_err(|e| e.to_string())?,
        Err(error) => serde_json::json!({"error":error}),
    };
    finish_research_archive(
        &source,
        &journal.run_id,
        if result.is_ok() {
            "completed"
        } else {
            "failed"
        },
        None,
        payload.clone(),
    )?;
    std::fs::write(
        &args[1],
        serde_json::to_vec_pretty(&payload).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    result?;
    println!("已保存运行 {}", journal.run_id);
    Ok(())
}
