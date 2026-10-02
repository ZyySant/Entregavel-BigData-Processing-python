"""
DataQualityFramework — validacoes customizadas para o pipeline ShopBrasil.

Nao usa Great Expectations nem Soda (proibido pela especificacao do projeto final).
E basicamente o framework do desafio da aula 6, adaptado para rodar dentro do
pipeline de producao ao inves de um notebook.
"""

import json
from dataclasses import dataclass, field, asdict
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, expr, lit, when


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)
    severity: str = "critical"  # critical | warning | info


class DataQualityFramework:
    """Roda checks de completude, unicidade, integridade referencial e validade
    sobre qualquer DataFrame, acumula os resultados e sabe separar registros
    bons dos ruins (quarentena)."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._results: List[CheckResult] = []

    def _register(self, result: CheckResult) -> CheckResult:
        self._results.append(result)
        return result

    def check_completeness(
        self, df: DataFrame, columns: List[str], threshold: float = 0.95,
        severity: str = "critical",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            return self._register(CheckResult(
                check_name=f"completeness_{'_'.join(columns)}",
                passed=False, metric_value=0.0, threshold=threshold,
                details={"motivo": "dataframe vazio"}, severity=severity,
            ))

        taxas = {}
        for c in columns:
            nao_nulos = df.filter(col(c).isNotNull()).count()
            taxas[c] = nao_nulos / total

        pior_coluna = min(taxas, key=taxas.get)
        pior_taxa = taxas[pior_coluna]

        return self._register(CheckResult(
            check_name=f"completeness_{'_'.join(columns)}",
            passed=pior_taxa >= threshold,
            metric_value=pior_taxa,
            threshold=threshold,
            details={"taxas_por_coluna": taxas, "pior_coluna": pior_coluna, "total_registros": total},
            severity=severity,
        ))

    def check_uniqueness(
        self, df: DataFrame, key_columns: List[str], severity: str = "critical",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            return self._register(CheckResult(
                check_name=f"uniqueness_{'_'.join(key_columns)}",
                passed=False, metric_value=0.0, threshold=1.0,
                details={"motivo": "dataframe vazio"}, severity=severity,
            ))

        distintos = df.select(key_columns).dropDuplicates().count()
        taxa_unicidade = distintos / total

        return self._register(CheckResult(
            check_name=f"uniqueness_{'_'.join(key_columns)}",
            passed=distintos == total,
            metric_value=taxa_unicidade,
            threshold=1.0,
            details={"total_registros": total, "distintos": distintos, "duplicados": total - distintos},
            severity=severity,
        ))

    def check_referential_integrity(
        self, df_source: DataFrame, df_reference: DataFrame,
        source_col: str, ref_col: str, threshold: float = 0.98,
        severity: str = "warning",
    ) -> CheckResult:
        total = df_source.count()
        if total == 0:
            return self._register(CheckResult(
                check_name=f"referential_integrity_{source_col}",
                passed=False, metric_value=0.0, threshold=threshold,
                details={"motivo": "dataframe vazio"}, severity=severity,
            ))

        refs_validas = df_source.join(
            df_reference.select(ref_col).distinct(),
            df_source[source_col] == df_reference[ref_col],
            "left_semi",
        ).count()
        taxa = refs_validas / total

        return self._register(CheckResult(
            check_name=f"referential_integrity_{source_col}",
            passed=taxa >= threshold,
            metric_value=taxa,
            threshold=threshold,
            details={"total_registros": total, "referencias_validas": refs_validas, "orfaos": total - refs_validas},
            severity=severity,
        ))

    def check_validity(
        self, df: DataFrame, rules: Dict[str, str], threshold: float = 0.95,
        severity: str = "warning",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            return self._register(CheckResult(
                check_name="validity", passed=False, metric_value=0.0, threshold=threshold,
                details={"motivo": "dataframe vazio"}, severity=severity,
            ))

        detalhes_por_regra = {}
        condicao_geral = lit(True)
        for nome, regra_sql in rules.items():
            condicao = expr(regra_sql)
            aprovados = df.filter(condicao).count()
            detalhes_por_regra[nome] = aprovados / total
            condicao_geral = condicao_geral & condicao

        validos = df.filter(condicao_geral).count()
        taxa_geral = validos / total

        return self._register(CheckResult(
            check_name="validity",
            passed=taxa_geral >= threshold,
            metric_value=taxa_geral,
            threshold=threshold,
            details={"regras": list(rules.keys()), "taxa_por_regra": detalhes_por_regra, "total_registros": total},
            severity=severity,
        ))

    def quarantine(self, df: DataFrame, rules: Dict[str, str]) -> Tuple[DataFrame, DataFrame]:
        """Separa em (validos, quarentena). Nada e descartado -- quem cai na
        quarentena carrega o motivo em 'quarantine_reasons' para investigacao."""
        df_marcado = df
        colunas_motivo = []
        for nome, regra_sql in rules.items():
            col_flag = f"_falhou_{nome}"
            df_marcado = df_marcado.withColumn(
                col_flag, when(~expr(regra_sql), lit(nome)).otherwise(lit(None))
            )
            colunas_motivo.append(col_flag)

        df_marcado = df_marcado.withColumn(
            "quarantine_reasons",
            concat_ws(",", *[col(c) for c in colunas_motivo]),
        ).drop(*colunas_motivo)

        df_validos = df_marcado.filter(col("quarantine_reasons") == "").drop("quarantine_reasons")
        df_quarentena = df_marcado.filter(col("quarantine_reasons") != "")

        return df_validos, df_quarentena

    def run_all_checks(self, df: DataFrame, config: Dict[str, Any],
                        df_reference: Optional[DataFrame] = None) -> List[CheckResult]:
        resultados = []

        if "completeness" in config:
            cfg = config["completeness"]
            resultados.append(self.check_completeness(
                df, cfg["columns"], cfg.get("threshold", 0.95), cfg.get("severity", "critical"),
            ))

        if "uniqueness" in config:
            cfg = config["uniqueness"]
            resultados.append(self.check_uniqueness(
                df, cfg["key_columns"], cfg.get("severity", "critical"),
            ))

        if "validity" in config:
            cfg = config["validity"]
            resultados.append(self.check_validity(
                df, cfg["rules"], cfg.get("threshold", 0.95), cfg.get("severity", "warning"),
            ))

        if "referential_integrity" in config and df_reference is not None:
            cfg = config["referential_integrity"]
            resultados.append(self.check_referential_integrity(
                df, df_reference, cfg["source_col"], cfg["ref_col"],
                cfg.get("threshold", 0.98), cfg.get("severity", "warning"),
            ))

        return resultados

    def generate_report(self) -> Dict[str, Any]:
        if not self._results:
            return {
                "checks_total": 0, "checks_passed": 0, "checks_failed": 0,
                "overall_score": 0.0, "gate_passed": False, "results": [],
            }

        checks_passed = sum(1 for r in self._results if r.passed)
        checks_failed = len(self._results) - checks_passed
        overall_score = sum(r.metric_value for r in self._results) / len(self._results)

        checks_criticos = [r for r in self._results if r.severity == "critical"]
        gate_passed = all(r.passed for r in checks_criticos) if checks_criticos else checks_passed > 0

        return {
            "checks_total": len(self._results),
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
            "overall_score": overall_score,
            "gate_passed": gate_passed,
            "results": [asdict(r) for r in self._results],
        }

    def reset(self) -> None:
        self._results = []

    def salvar_relatorio(self, caminho: str) -> None:
        """Grava o relatorio consolidado em JSON -- usado pela task de quality
        gate no Airflow, que le esse arquivo sem precisar subir uma sessao Spark."""
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(self.generate_report(), f, ensure_ascii=False, indent=2, default=str)
