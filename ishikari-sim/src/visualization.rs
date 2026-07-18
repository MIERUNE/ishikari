use std::{fs, io::Write, path::Path};

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use crate::{ensure_output_distinct, write_atomic};

const TEMPLATE: &str = include_str!("visualization.html");
const REPORT_PLACEHOLDER: &str = "__ISHIKARI_REPORT_JSON__";

pub fn write_visualization(input: impl AsRef<Path>, output: impl AsRef<Path>) -> Result<()> {
    let input = input.as_ref();
    let output = output.as_ref();
    ensure_output_distinct(output, [input]).context("validate visualization output path")?;
    let bytes = fs::read(input).with_context(|| format!("read report {}", input.display()))?;
    let report: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse report {}", input.display()))?;
    let html = render_visualization(&report)?;
    write_atomic(output, |writer| {
        writer.write_all(html.as_bytes())?;
        Ok(())
    })
    .with_context(|| format!("write visualization {}", output.display()))
}

pub fn render_visualization(report: &Value) -> Result<String> {
    ensure!(report.get("result").is_some(), "report is missing result");
    let embedded = serde_json::to_string(report)
        .context("serialize report for visualization")?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    Ok(TEMPLATE.replace(REPORT_PLACEHOLDER, &embedded))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render_visualization;

    #[test]
    fn embeds_report_without_allowing_script_termination() {
        let report = json!({
            "execution_mode": "churn",
            "trace": {"kind": "replay", "input": "</script><script>alert(1)</script>"},
            "cluster": {"node_count": 3},
            "result": {"requests": 10, "nodes": []}
        });

        let html = render_visualization(&report).expect("visualization");

        assert!(html.contains("Ishikari Simulator"));
        assert!(html.contains("Final Node State"));
        assert!(!html.contains("sampleRange"));
        assert!(!html.contains("</script><script>alert(1)</script>"));
        assert!(html.contains("\\u003c/script\\u003e"));
    }

    #[test]
    fn rejects_non_report_json() {
        assert!(render_visualization(&json!({"requests": 1})).is_err());
    }
}
