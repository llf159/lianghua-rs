import { useState } from "react";
import { save } from "@tauri-apps/plugin-dialog";
import { exportResearchArchive } from "../../../apis/strategyTrigger";
export function ResearchExportButton({sourcePath, runId, disabled}: {sourcePath: string; runId: string; disabled?: boolean}) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  async function exportRun() {
    setBusy(true); setMessage("");
    try {
      const path = await save({title: "导出研究结果", defaultPath: `${runId}.lhresearch`, filters: [{name: "研究结果包", extensions: ["lhresearch"]}]});
      if (path) {await exportResearchArchive(sourcePath,runId,path); setMessage("结果已导出。");}
    } catch (failure) {setMessage(`导出失败：${String(failure)}`);}
    finally {setBusy(false);}
  }
  return <span className="research-transfer"><button className="research-button" type="button" disabled={disabled || busy} onClick={() => void exportRun()}>{busy ? "导出中…" : "导出结果"}</button>{message ? <span role="status"> {message}</span> : null}</span>;
}
