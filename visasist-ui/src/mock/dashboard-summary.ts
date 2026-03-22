export interface LotHealth {
  lot: string; total: number; overdue: number; overdue_pct: number;
}

export interface DashboardSummary {
  pipeline_run_id: string; run_at: string; lot_count: number; doc_count: number;
  kpis: { total_docs: number; pending_visa: number; overdue: number; easy_wins: number; blocked: number; conflicts: number; };
  kpi_deltas: { pending_visa: number; overdue: number; easy_wins: number; blocked: number; conflicts: number; };
  category_counts: Record<string, number>; lot_health_top5: LotHealth[];
  main_blocker: string; urgent_item_ids: string[]; recent_anomaly_ids: number[]; top_recommendation_ids: string[];
}

export const dashboardSummary: DashboardSummary = {
  "pipeline_run_id": "run-2026-03-23",
  "run_at": "2026-03-23T00:00:00Z",
  "lot_count": 82,
  "doc_count": 2392,
  "kpis": {
    "total_docs": 2392,
    "pending_visa": 1852,
    "overdue": 2255,
    "easy_wins": 82,
    "blocked": 1,
    "conflicts": 18
  },
  "kpi_deltas": {
    "pending_visa": 0,
    "overdue": 0,
    "easy_wins": 0,
    "blocked": 0,
    "conflicts": 0
  },
  "category_counts": {
    "CONFLICT": 18,
    "NOT_STARTED": 953,
    "EASY_WIN_APPROVE": 82,
    "WAITING": 1338,
    "BLOCKED": 1
  },
  "lot_health_top5": [
    {
      "lot": "I003",
      "total": 122,
      "overdue": 110,
      "overdue_pct": 90.2
    },
    {
      "lot": "B003",
      "total": 110,
      "overdue": 109,
      "overdue_pct": 99.1
    },
    {
      "lot": "B031",
      "total": 97,
      "overdue": 91,
      "overdue_pct": 93.8
    },
    {
      "lot": "H031",
      "total": 95,
      "overdue": 95,
      "overdue_pct": 100.0
    },
    {
      "lot": "I042",
      "total": 90,
      "overdue": 90,
      "overdue_pct": 100.0
    }
  ],
  "main_blocker": "GEMO_MOEX",
  "urgent_item_ids": [
    "P17_T2_IN_EXE_LGD_GOE_I003_ARM_I1_S5_028176::C::8",
    "P17_T2_GE_EXE_LGD_GOE_I003_NDC_TZ_TX_028000::A::2",
    "P17_T2_IN_EXE_LGD_GOE_I003_ARM_I1_S4_028196::A::2",
    "P17_T2_IN_EXE_LGD_GOE_I003_ARM_I2_S5_028179::B::4",
    "P17_T2_IN_EXE_LGD_GOE_I003_ARM_I1_S4_028199::C::6",
    "P17_T2_BX_EXE_BEN_CFO_B031_SYQ_BZ_R0_145425::A::2"
  ],
  "recent_anomaly_ids": [
    1,
    2,
    3,
    4,
    5
  ],
  "top_recommendation_ids": []
};
