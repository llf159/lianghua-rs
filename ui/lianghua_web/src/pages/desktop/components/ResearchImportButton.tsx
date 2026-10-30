import { useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { importResearchArchive, type ResearchArchiveHeader } from "../../../apis/strategyTrigger";
export function ResearchImportButton({sourcePath, disabled, onImported}: {sourcePath: string; disabled?: boolean; onImported: (header: ResearchArchiveHeader) => void}) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  async function choose() {
    setBusy(true); setMessage("");
    try {
      const path = await open({title: "导入研究结果包", multiple: false, directory: false, filters: [{name: "研究结果包", extensions: ["lhresearch"]}]});
      if (typeof path === "string") {
        const header = await importResearchArchive(sourcePath, path);
        setMessage(`结果已导入。`);
        onImported(header);
      }
    } catch (failure) {setMessage(`导入失败：${String(failure)}`);}
    finally {setBusy(false);}
  }
  return <span className="research-transfer"><button className="research-button" type="button" disabled={disabled || busy || !sourcePath} onClick={() => void choose()}>{busy ? "导入中…" : "导入结果"}</button>{message ? <span role="status"> {message}</span> : null}</span>;
}
